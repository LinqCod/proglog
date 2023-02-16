package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var enc = binary.BigEndian

const dataLengthWeightInBytes = 8

type store struct {
	*os.File
	mutex    sync.Mutex
	buffer   *bufio.Writer
	fileSize uint64
}

func newStore(file *os.File) (*store, error) {
	fileInfo, err := os.Stat(file.Name())
	if err != nil {
		return nil, err
	}

	fileSize := uint64(fileInfo.Size())

	return &store{
		File:     file,
		fileSize: fileSize,
		buffer:   bufio.NewWriter(file),
	}, nil
}

// Append method appends data to the store file
// returns written data length in bytes, start position of logged data and error
func (s *store) Append(data []byte) (n uint64, pos uint64, err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	pos = s.fileSize

	// writing data length to file (takes 8 bytes == 'dataLengthWeightInBytes' const)
	if err = binary.Write(s.buffer, enc, uint64(len(data))); err != nil {
		return 0, 0, err
	}

	// writing data to file (takes w bytes)
	w, err := s.buffer.Write(data)
	if err != nil {
		return 0, 0, err
	}

	// summarize file's space taken by written data + len(data)
	w += dataLengthWeightInBytes

	s.fileSize += uint64(w)

	return uint64(w), pos, nil
}

// Read method reads data from store file starting at pos
// returns log data starting at pos and error
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// flushing any data from buffer to store file if it is not already there
	if err := s.buffer.Flush(); err != nil {
		return nil, err
	}

	length := make([]byte, dataLengthWeightInBytes)
	// reading length of the log data starting at pos
	if _, err := s.File.ReadAt(length, int64(pos)); err != nil {
		return nil, err
	}

	b := make([]byte, enc.Uint64(length))
	// reading log data with size of length starting from pos + dataLengthWeightInBytes
	if _, err := s.File.ReadAt(b, int64(pos+dataLengthWeightInBytes)); err != nil {
		return nil, err
	}

	return b, nil
}

// ReadAt method reads len(data) bytes from store file into data beginning at the given offset
// returns written data length in bytes
func (s *store) ReadAt(data []byte, offset int64) (int, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// flushing any data from buffer to store file if it is not already there
	if err := s.buffer.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(data, offset)
}

func (s *store) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// flushing any data from buffer to store file if it is not already there
	if err := s.buffer.Flush(); err != nil {
		return err
	}

	return s.File.Close()
}