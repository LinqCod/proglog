package log

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

var (
	testData       = []byte("hello world log")
	testDataLength = uint64(len(testData)) + dataLengthWeightInBytes
)

func TestStoreAppendRead(t *testing.T) {
	f, err := os.CreateTemp("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	s, err = newStore(f)
	require.NoError(t, err)

	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()

	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(testData)
		require.NoError(t, err)
		require.Equal(t, pos+n, testDataLength*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()

	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, testData, read)
		pos += testDataLength
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()

	for i, offset := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, dataLengthWeightInBytes)
		n, err := s.ReadAt(b, offset)
		require.NoError(t, err)
		require.Equal(t, dataLengthWeightInBytes, n)
		offset += int64(n)

		size := enc.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, offset)
		require.NoError(t, err)
		require.Equal(t, testData, b)
		require.Equal(t, int(size), n)
		offset += int64(n)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := os.CreateTemp("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	_, _, err = s.Append(testData)
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	f, afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	require.True(t, afterSize > beforeSize)
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, 0, err
	}

	fileInfo, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}

	return f, fileInfo.Size(), nil
}