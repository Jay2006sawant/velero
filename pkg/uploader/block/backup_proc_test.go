/*
Copyright The Velero Contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package block

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/vmware-tanzu/velero/pkg/repository/udmrepo"
	udmrepomocks "github.com/vmware-tanzu/velero/pkg/repository/udmrepo/mocks"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	cbt "github.com/vmware-tanzu/velero/pkg/uploader/cbt/types"
	cbtmocks "github.com/vmware-tanzu/velero/pkg/uploader/cbt/types/mocks"
	"github.com/vmware-tanzu/velero/pkg/util/freelist"
)

type writeRecord struct {
	offset int64
	length int
	data   []byte
}

type capturingObjectWriter struct {
	mu      sync.Mutex
	writes  []writeRecord
	writeFn func(p []byte, off int64) (int, error)
}

func (w *capturingObjectWriter) WriteAt(p []byte, off int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := writeRecord{
		offset: off,
		length: len(p),
		data:   append([]byte(nil), p...),
	}
	w.writes = append(w.writes, rec)

	if w.writeFn != nil {
		return w.writeFn(p, off)
	}
	return len(p), nil
}

func (w *capturingObjectWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func (w *capturingObjectWriter) Checkpoint() (udmrepo.ID, error) {
	return udmrepo.ID(""), nil
}

func (w *capturingObjectWriter) Close() error {
	return nil
}

func (w *capturingObjectWriter) Result() (udmrepo.ID, error) {
	return udmrepo.ID("test-obj"), nil
}

func TestReadResultWriteLength(t *testing.T) {
	testCases := []struct {
		name        string
		result      readResult
		blockSize   int
		dataLength  int64
		expectedLen int64
	}{
		{
			name:        "uses explicit length from read",
			result:      readResult{offset: 1024, length: 512},
			blockSize:   1024,
			dataLength:  1536,
			expectedLen: 512,
		},
		{
			name:        "falls back to full block",
			result:      readResult{offset: 0, length: 0},
			blockSize:   1024,
			dataLength:  2048,
			expectedLen: 1024,
		},
		{
			name:        "falls back to partial tail at end",
			result:      readResult{offset: 1024, length: 0},
			blockSize:   1024,
			dataLength:  1536,
			expectedLen: 512,
		},
		{
			name:        "explicit length overrides fallback",
			result:      readResult{offset: 0, length: 256},
			blockSize:   1024,
			dataLength:  2048,
			expectedLen: 256,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectedLen, readResultWriteLength(tc.result, tc.blockSize, tc.dataLength))
		})
	}
}

func TestBackupWriteProcPartialLastBlock(t *testing.T) {
	const blockSize = 1024
	const dataLength int64 = 1536
	const alignedLength int64 = 2048

	ctx := context.Background()
	writer := &capturingObjectWriter{}
	list := freelist.New(4*blockSize, blockSize)
	resultChan := make(chan readResult, 4)
	progress := &mockProgressUpdater{}
	progress.On("UpdateProgress", mock.Anything).Return()

	go func() {
		buf1 := list.Get()
		for i := range buf1 {
			buf1[i] = 0xAA
		}
		resultChan <- readResult{buffer: buf1, offset: 0, length: blockSize}

		buf2 := list.Get()
		for i := range buf2[:512] {
			buf2[i] = 0xBB
		}
		resultChan <- readResult{buffer: buf2, offset: blockSize, length: 512}
		close(resultChan)
	}()

	written, lastPos, err := backupWriteProc(ctx, writer, resultChan, list, dataLength, alignedLength, 2, blockSize, progress)
	require.NoError(t, err)
	assert.Equal(t, dataLength, written)
	assert.Equal(t, dataLength, lastPos)

	require.Len(t, writer.writes, 2)
	assert.Equal(t, int64(0), writer.writes[0].offset)
	assert.Equal(t, blockSize, writer.writes[0].length)
	assert.Equal(t, int64(blockSize), writer.writes[1].offset)
	assert.Equal(t, 512, writer.writes[1].length)
}

func TestBackupWriteProcShortWrite(t *testing.T) {
	const blockSize = 1024

	ctx := context.Background()
	writer := &capturingObjectWriter{
		writeFn: func(p []byte, off int64) (int, error) {
			return len(p) / 2, nil
		},
	}
	list := freelist.New(2*blockSize, blockSize)
	resultChan := make(chan readResult, 2)
	progress := &mockProgressUpdater{}

	go func() {
		buf := list.Get()
		resultChan <- readResult{buffer: buf, offset: 0, length: blockSize}
		close(resultChan)
	}()

	_, _, err := backupWriteProc(ctx, writer, resultChan, list, 1024, 1024, 1, blockSize, progress)
	require.Error(t, err)
	assert.ErrorIs(t, err, io.ErrShortWrite)
}

func TestBackupWriteProcCanceled(t *testing.T) {
	const blockSize = 1024

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	writer := &capturingObjectWriter{}
	list := freelist.New(2*blockSize, blockSize)
	resultChan := make(chan readResult)
	progress := &mockProgressUpdater{}

	_, _, err := backupWriteProc(ctx, writer, resultChan, list, 1024, 1024, 1, blockSize, progress)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCanceled)
}

func TestBackupReadProcPartialLastBlockLength(t *testing.T) {
	const blockSize = 1024
	const totalLength int64 = 1536

	source := make([]byte, totalLength)
	for i := range source {
		source[i] = byte(i % 256)
	}

	iterMock := cbtmocks.NewIterator(t)
	iterMock.On("BlockSize").Return(uint(blockSize))
	iterMock.On("Next").Return(uint64(0), true).Once()
	iterMock.On("Next").Return(uint64(blockSize), true).Once()
	iterMock.On("Next").Return(uint64(0), false)

	list := freelist.New(4*blockSize, blockSize)
	resultChan := make(chan readResult, 4)
	quit := make(chan struct{})
	defer close(quit)

	go backupReadProc(context.Background(), bytes.NewReader(source), resultChan, quit, iterMock, list, totalLength)

	var results []readResult
	for r := range resultChan {
		require.NoError(t, r.err)
		results = append(results, r)
	}

	require.Len(t, results, 2)
	assert.Equal(t, blockSize, results[0].length)
	assert.Equal(t, 512, results[1].length)
	assert.Equal(t, int64(0), results[0].offset)
	assert.Equal(t, int64(blockSize), results[1].offset)
}

func TestBackupDataNonAlignedVolume(t *testing.T) {
	const blockSize = 1024
	const totalLength int64 = 1536

	source := make([]byte, totalLength)
	for i := range source {
		source[i] = byte(i % 251)
	}

	writer := &capturingObjectWriter{}
	iterMock := cbtmocks.NewIterator(t)
	iterMock.On("BlockSize").Return(uint(blockSize))
	iterMock.On("Count").Return(uint64(2))
	iterMock.On("Next").Return(uint64(0), true).Once()
	iterMock.On("Next").Return(uint64(blockSize), true).Once()
	iterMock.On("Next").Return(uint64(0), false)

	blkup := &blockUploader{
		ctx:      context.Background(),
		progress: &mockProgressUpdater{},
		log:      logrus.New(),
	}
	blkup.progress.(*mockProgressUpdater).On("UpdateProgress", mock.Anything).Return()

	written, aligned, err := blkup.backupData(bytes.NewReader(source), writer, iterMock, totalLength)
	require.NoError(t, err)
	assert.Equal(t, int64(2048), aligned)
	assert.Equal(t, totalLength, written)

	require.Len(t, writer.writes, 2)
	assert.Equal(t, blockSize, writer.writes[0].length)
	assert.Equal(t, 512, writer.writes[1].length)
	assert.Equal(t, source[:blockSize], writer.writes[0].data[:blockSize])
	assert.Equal(t, source[blockSize:], writer.writes[1].data[:512])
}

func TestBackupDataSkipsCopyTailWhenPartialBlockWritten(t *testing.T) {
	const blockSize = 1024
	const totalLength int64 = 1536

	source := make([]byte, totalLength)
	writer := &capturingObjectWriter{}

	iterMock := cbtmocks.NewIterator(t)
	iterMock.On("BlockSize").Return(uint(blockSize))
	iterMock.On("Count").Return(uint64(2))
	iterMock.On("Next").Return(uint64(0), true).Once()
	iterMock.On("Next").Return(uint64(blockSize), true).Once()
	iterMock.On("Next").Return(uint64(0), false)

	blkup := &blockUploader{
		ctx:      context.Background(),
		progress: &mockProgressUpdater{},
		log:      logrus.New(),
	}
	blkup.progress.(*mockProgressUpdater).On("UpdateProgress", mock.Anything).Return()

	_, _, err := blkup.backupData(bytes.NewReader(source), writer, iterMock, totalLength)
	require.NoError(t, err)
	require.Len(t, writer.writes, 2, "copyTailData must not add a duplicate tail write")
	assert.Equal(t, totalLength, writer.writes[1].offset+int64(writer.writes[1].length))
}

func TestRestoreWriteProcUsesReadLength(t *testing.T) {
	const blockSize = 1024
	const totalLength int64 = 1536

	ctx := context.Background()
	destFile, err := os.CreateTemp(t.TempDir(), "restore-partial-*")
	require.NoError(t, err)
	defer destFile.Close()

	require.NoError(t, destFile.Truncate(totalLength))

	list := freelist.New(4*blockSize, blockSize)
	resultChan := make(chan readResult, 4)
	progress := &mockProgressUpdater{}
	progress.On("UpdateProgress", mock.Anything).Return()
	log := logrus.New()
	log.Out = io.Discard

	go func() {
		buf1 := list.Get()
		for i := range buf1 {
			buf1[i] = 0x01
		}
		resultChan <- readResult{buffer: buf1, offset: 0, length: blockSize}

		buf2 := list.Get()
		for i := range buf2[:512] {
			buf2[i] = 0x02
		}
		resultChan <- readResult{buffer: buf2, offset: blockSize, length: 512}
		close(resultChan)
	}()

	written, err := restoreWriteProc(ctx, destFile, resultChan, list, totalLength, 2, blockSize, destFile.Name(), progress, log)
	require.NoError(t, err)
	assert.Equal(t, totalLength, written)

	data, err := io.ReadAll(destFile)
	require.NoError(t, err)
	assert.Equal(t, totalLength, int64(len(data)))
	assert.Equal(t, byte(0x01), data[0])
	assert.Equal(t, byte(0x02), data[blockSize])
}

func TestBlockUploaderRestoreDestinationTooSmall(t *testing.T) {
	ctx := context.Background()
	repoWriter := udmrepomocks.NewBackupRepo(t)
	blkup := NewUploader(ctx, repoWriter, nil, logrus.New())

	meta := &udmrepo.Metadata{
		SubObjects: []udmrepo.ObjectMetadata{
			{ID: "data-id", Name: "bdev", Size: 1048576},
		},
	}
	repoWriter.On("ReadMetadata", mock.Anything, udmrepo.ID("root-id")).Return(meta, nil)

	iterMock := cbtmocks.NewIterator(t)
	snap := udmrepo.Snapshot{
		RootObject: udmrepo.ObjectMetadata{ID: "root-id"},
		Tags:       map[string]string{bdevSourceSizeTag: "1048576"},
	}
	dest := destInfo{size: 512, path: "/dev/small"}

	_, err := blkup.Restore(snap, dest, iterMock, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dest dev(/dev/small) size is too small")
	assert.NotContains(t, err.Error(), "wrap")
}

func TestBlockUploaderRestoreUnexpectedObjectSize(t *testing.T) {
	ctx := context.Background()
	repoWriter := udmrepomocks.NewBackupRepo(t)
	blkup := NewUploader(ctx, repoWriter, nil, logrus.New())

	meta := &udmrepo.Metadata{
		SubObjects: []udmrepo.ObjectMetadata{
			{ID: "data-id", Name: "bdev", Size: 512},
		},
	}
	repoWriter.On("ReadMetadata", mock.Anything, udmrepo.ID("root-id")).Return(meta, nil)

	iterMock := cbtmocks.NewIterator(t)
	snap := udmrepo.Snapshot{
		RootObject: udmrepo.ObjectMetadata{ID: "root-id"},
		Tags:       map[string]string{bdevSourceSizeTag: "1048576"},
	}
	dest := destInfo{size: 2048576, path: "/dev/large"}

	_, err := blkup.Restore(snap, dest, iterMock, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected size (512 vs. 1048576) for bdev object bdev")
}

func TestBackupWriteProcMultipleFullBlocks(t *testing.T) {
	const blockSize = 512
	const dataLength int64 = 2048

	ctx := context.Background()
	writer := &capturingObjectWriter{}
	list := freelist.New(8*blockSize, blockSize)
	resultChan := make(chan readResult, 8)
	progress := &mockProgressUpdater{}
	progress.On("UpdateProgress", mock.Anything).Return()

	go func() {
		for i := 0; i < 4; i++ {
			buf := list.Get()
			buf[0] = byte(i)
			resultChan <- readResult{
				buffer: buf,
				offset: int64(i * blockSize),
				length: blockSize,
			}
		}
		close(resultChan)
	}()

	written, lastPos, err := backupWriteProc(ctx, writer, resultChan, list, dataLength, dataLength, 4, blockSize, progress)
	require.NoError(t, err)
	assert.Equal(t, dataLength, written)
	assert.Equal(t, dataLength, lastPos)
	require.Len(t, writer.writes, 4)
	for i, w := range writer.writes {
		assert.Equal(t, blockSize, w.length)
		assert.Equal(t, int64(i*blockSize), w.offset)
	}
}

func TestBackupWriteProcReadError(t *testing.T) {
	const blockSize = 1024

	ctx := context.Background()
	writer := &capturingObjectWriter{}
	list := freelist.New(2*blockSize, blockSize)
	resultChan := make(chan readResult, 2)
	progress := &mockProgressUpdater{}
	progress.On("UpdateProgress", mock.Anything).Return()

	readErr := errors.New("read failed")
	go func() {
		buf := list.Get()
		resultChan <- readResult{buffer: buf, offset: 0, length: blockSize, err: readErr}
		close(resultChan)
	}()

	_, _, err := backupWriteProc(ctx, writer, resultChan, list, 1024, 1024, 1, blockSize, progress)
	require.Error(t, err)
	assert.ErrorContains(t, err, "read failed")
}

func TestBackupWriteProcUnexpectedEOF(t *testing.T) {
	const blockSize = 1024

	ctx := context.Background()
	writer := &capturingObjectWriter{}
	list := freelist.New(2*blockSize, blockSize)
	resultChan := make(chan readResult, 2)
	progress := &mockProgressUpdater{}
	progress.On("UpdateProgress", mock.Anything).Return()

	go func() {
		buf := list.Get()
		resultChan <- readResult{buffer: buf, offset: 0, length: blockSize}
		close(resultChan)
	}()

	_, _, err := backupWriteProc(ctx, writer, resultChan, list, 2048, 2048, 2, blockSize, progress)
	require.Error(t, err)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestBackupReadProcContextCanceled(t *testing.T) {
	const blockSize = 1024

	iterMock := cbtmocks.NewIterator(t)
	iterMock.On("BlockSize").Return(uint(blockSize))
	iterMock.On("Next").Return(uint64(0), true)

	list := freelist.New(2*blockSize, blockSize)
	resultChan := make(chan readResult, 2)
	quit := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	go backupReadProc(ctx, bytes.NewReader(make([]byte, 2048)), resultChan, quit, iterMock, list, 2048)

	var count int
	for range resultChan {
		count++
	}
	assert.Equal(t, 0, count)
}

func TestRestoreWriteProcZeroBlockPartialLength(t *testing.T) {
	const blockSize = 1024
	const totalLength int64 = 1536

	ctx := context.Background()
	destFile, err := os.CreateTemp(t.TempDir(), "restore-zero-partial-*")
	require.NoError(t, err)
	defer destFile.Close()
	require.NoError(t, destFile.Truncate(totalLength))

	list := freelist.New(4*blockSize, blockSize)
	resultChan := make(chan readResult, 4)
	progress := &mockProgressUpdater{}
	progress.On("UpdateProgress", mock.Anything).Return()
	log := logrus.New()
	log.Out = io.Discard

	go func() {
		buf1 := list.Get()
		buf1[0] = 0xFF
		resultChan <- readResult{buffer: buf1, offset: 0, length: blockSize}

		buf2 := list.Get()
		resultChan <- readResult{buffer: buf2, offset: blockSize, length: 512}
		close(resultChan)
	}()

	written, err := restoreWriteProc(ctx, destFile, resultChan, list, totalLength, 2, blockSize, destFile.Name(), progress, log)
	require.NoError(t, err)
	assert.Equal(t, totalLength, written)

	data, err := io.ReadAll(destFile)
	require.NoError(t, err)
	assert.Equal(t, byte(0xFF), data[0])
	assert.Equal(t, byte(0), data[blockSize])
}

func TestBackupDataWithPartialThirdBlock(t *testing.T) {
	const blockSize = 1024
	const totalLength int64 = 2560

	source := make([]byte, totalLength)
	for i := range source {
		source[i] = byte(i % 200)
	}

	writer := &capturingObjectWriter{}
	iterMock := cbtmocks.NewIterator(t)
	iterMock.On("BlockSize").Return(uint(blockSize))
	iterMock.On("Count").Return(uint64(3))
	iterMock.On("Next").Return(uint64(0), true).Once()
	iterMock.On("Next").Return(uint64(blockSize), true).Once()
	iterMock.On("Next").Return(uint64(2*blockSize), true).Once()
	iterMock.On("Next").Return(uint64(0), false)

	progress := &mockProgressUpdater{}
	progress.On("UpdateProgress", mock.Anything).Return()

	blkup := &blockUploader{
		ctx:      context.Background(),
		progress: progress,
		log:      logrus.New(),
	}

	written, aligned, err := blkup.backupData(bytes.NewReader(source), writer, iterMock, totalLength)
	require.NoError(t, err)
	assert.Equal(t, int64(3072), aligned)
	assert.Equal(t, totalLength, written)
	require.Len(t, writer.writes, 3)
	assert.Equal(t, blockSize, writer.writes[0].length)
	assert.Equal(t, blockSize, writer.writes[1].length)
	assert.Equal(t, 512, writer.writes[2].length)
}

func TestBackupWriteProcProgressUsesAlignedLength(t *testing.T) {
	const blockSize = 1024
	const dataLength int64 = 1536
	const alignedLength int64 = 2048

	ctx := context.Background()
	writer := &capturingObjectWriter{}
	list := freelist.New(4*blockSize, blockSize)
	resultChan := make(chan readResult, 4)
	progress := &mockProgressUpdater{}

	var lastProgress uploader.Progress
	progress.On("UpdateProgress", mock.Anything).Run(func(args mock.Arguments) {
		p := args.Get(0).(*uploader.Progress)
		lastProgress = *p
	}).Return()

	go func() {
		buf1 := list.Get()
		resultChan <- readResult{buffer: buf1, offset: 0, length: blockSize}
		buf2 := list.Get()
		resultChan <- readResult{buffer: buf2, offset: blockSize, length: 512}
		close(resultChan)
	}()

	_, _, err := backupWriteProc(ctx, writer, resultChan, list, dataLength, alignedLength, 2, blockSize, progress)
	require.NoError(t, err)
	assert.Equal(t, alignedLength, lastProgress.TotalBytes)
	assert.Equal(t, dataLength, lastProgress.BytesDone)
}

func setupSequentialIterator(t *testing.T, blockSize uint, offsets ...uint64) cbt.Iterator {
	t.Helper()
	iterMock := cbtmocks.NewIterator(t)
	iterMock.On("BlockSize").Return(blockSize)
	for _, off := range offsets {
		iterMock.On("Next").Return(off, true).Once()
	}
	iterMock.On("Next").Return(uint64(0), false)
	return iterMock
}

func TestBackupReadProcSequentialOffsets(t *testing.T) {
	const blockSize = 512
	const totalLength int64 = 1280

	source := make([]byte, totalLength)
	for i := range source {
		source[i] = byte(i)
	}

	iter := setupSequentialIterator(t, blockSize, 0, 512, 1024)
	list := freelist.New(4*blockSize, blockSize)
	resultChan := make(chan readResult, 4)
	quit := make(chan struct{})
	defer close(quit)

	go backupReadProc(context.Background(), bytes.NewReader(source), resultChan, quit, iter, list, totalLength)

	var lengths []int
	for r := range resultChan {
		require.NoError(t, r.err)
		lengths = append(lengths, r.length)
	}

	assert.Equal(t, []int{512, 512, 256}, lengths)
}
