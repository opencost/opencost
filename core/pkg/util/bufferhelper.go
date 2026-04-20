package util

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"math"
)

func readBool(r *bytes.Buffer, data *bool) error {
	b, err := r.ReadByte()
	if err != nil {
		return err
	}

	*data = b != 0
	return nil
}

func readInt8(r *bytes.Buffer, data *int8) error {
	b, err := r.ReadByte()
	if err != nil {
		return err
	}

	*data = int8(b)
	return nil
}

func readUint8(r *bytes.Buffer, data *uint8) error {
	b, err := r.ReadByte()
	if err != nil {
		return err
	}

	*data = uint8(b)
	return nil
}

func readInt16(r *bytes.Buffer, data *int16) error {
	order := binary.LittleEndian
	var b [2]byte

	bs := b[:]
	_, err := readFull(r, bs)
	if err != nil {
		return err
	}

	*data = int16(order.Uint16(bs))
	return nil
}

func readUint16(r *bytes.Buffer, data *uint16) error {
	order := binary.LittleEndian
	var b [2]byte

	bs := b[:]
	_, err := readFull(r, bs)
	if err != nil {
		return err
	}

	*data = order.Uint16(bs)
	return nil
}

func readInt(r *bytes.Buffer, data *int) error {
	order := binary.LittleEndian
	var b [4]byte

	bs := b[:]
	_, err := readFull(r, bs)
	if err != nil {
		return err
	}

	*data = int(int32(order.Uint32(bs)))
	return nil
}

func readInt32(r *bytes.Buffer, data *int32) error {
	order := binary.LittleEndian
	var b [4]byte

	bs := b[:]
	_, err := readFull(r, bs)
	if err != nil {
		return err
	}

	*data = int32(order.Uint32(bs))
	return nil
}

func readUint(r *bytes.Buffer, data *uint) error {
	order := binary.LittleEndian
	var b [4]byte

	bs := b[:]
	_, err := readFull(r, bs)
	if err != nil {
		return err
	}

	*data = uint(order.Uint32(bs))
	return nil
}

func readUint32(r *bytes.Buffer, data *uint32) error {
	order := binary.LittleEndian
	var b [4]byte

	bs := b[:]
	_, err := readFull(r, bs)
	if err != nil {
		return err
	}

	*data = order.Uint32(bs)
	return nil
}

func readInt64(r *bytes.Buffer, data *int64) error {
	order := binary.LittleEndian
	var b [8]byte

	bs := b[:]
	_, err := readFull(r, bs)
	if err != nil {
		return err
	}

	*data = int64(order.Uint64(bs))
	return nil
}

func readUint64(r *bytes.Buffer, data *uint64) error {
	order := binary.LittleEndian
	var b [8]byte

	bs := b[:]
	_, err := readFull(r, bs)
	if err != nil {
		return err
	}

	*data = order.Uint64(bs)
	return nil
}

func readFloat32(r *bytes.Buffer, data *float32) error {
	order := binary.LittleEndian
	var b [4]byte

	bs := b[:]
	_, err := readFull(r, bs)
	if err != nil {
		return err
	}

	*data = math.Float32frombits(order.Uint32(bs))
	return nil
}

func readFloat64(r *bytes.Buffer, data *float64) error {
	order := binary.LittleEndian
	var b [8]byte

	bs := b[:]
	_, err := readFull(r, bs)
	if err != nil {
		return err
	}

	*data = math.Float64frombits(order.Uint64(bs))
	return nil
}

func readBuffBool(r *bufio.Reader, data *bool) error {
	b, err := r.ReadByte()
	if err != nil {
		return err
	}

	*data = b != 0
	return nil
}

func readBuffInt8(r *bufio.Reader, data *int8) error {
	b, err := r.ReadByte()
	if err != nil {
		return err
	}

	*data = int8(b)
	return nil
}

func readBuffUint8(r *bufio.Reader, data *uint8) error {
	b, err := r.ReadByte()
	if err != nil {
		return err
	}

	*data = uint8(b)
	return nil
}

func readBuffInt16(r *bufio.Reader, data *int16) error {
	order := binary.LittleEndian
	var b [2]byte

	bs := b[:]
	_, err := readBuffFull(r, bs)
	if err != nil {
		return err
	}

	*data = int16(order.Uint16(bs))
	return nil
}

func readBuffUint16(r *bufio.Reader, data *uint16) error {
	order := binary.LittleEndian
	var b [2]byte

	bs := b[:]
	_, err := readBuffFull(r, bs)
	if err != nil {
		return err
	}

	*data = order.Uint16(bs)
	return nil
}

func readBuffInt(r *bufio.Reader, data *int) error {
	order := binary.LittleEndian
	var b [4]byte

	bs := b[:]
	_, err := readBuffFull(r, bs)
	if err != nil {
		return err
	}

	*data = int(int32(order.Uint32(bs)))
	return nil
}

func readBuffInt32(r *bufio.Reader, data *int32) error {
	order := binary.LittleEndian
	var b [4]byte

	bs := b[:]
	_, err := readBuffFull(r, bs)
	if err != nil {
		return err
	}

	*data = int32(order.Uint32(bs))
	return nil
}

func readBuffUint(r *bufio.Reader, data *uint) error {
	order := binary.LittleEndian
	var b [4]byte

	bs := b[:]
	_, err := readBuffFull(r, bs)
	if err != nil {
		return err
	}

	*data = uint(order.Uint32(bs))
	return nil
}

func readBuffUint32(r *bufio.Reader, data *uint32) error {
	order := binary.LittleEndian
	var b [4]byte

	bs := b[:]
	_, err := readBuffFull(r, bs)
	if err != nil {
		return err
	}

	*data = order.Uint32(bs)
	return nil
}

func readBuffInt64(r *bufio.Reader, data *int64) error {
	order := binary.LittleEndian
	var b [8]byte

	bs := b[:]
	_, err := readBuffFull(r, bs)
	if err != nil {
		return err
	}

	*data = int64(order.Uint64(bs))
	return nil
}

func readBuffUint64(r *bufio.Reader, data *uint64) error {
	order := binary.LittleEndian
	var b [8]byte

	bs := b[:]
	_, err := readBuffFull(r, bs)
	if err != nil {
		return err
	}

	*data = order.Uint64(bs)
	return nil
}

func readBuffFloat32(r *bufio.Reader, data *float32) error {
	order := binary.LittleEndian
	var b [4]byte

	bs := b[:]
	_, err := readBuffFull(r, bs)
	if err != nil {
		return err
	}

	*data = math.Float32frombits(order.Uint32(bs))
	return nil
}

func readBuffFloat64(r *bufio.Reader, data *float64) error {
	order := binary.LittleEndian
	var b [8]byte

	bs := b[:]
	_, err := readBuffFull(r, bs)
	if err != nil {
		return err
	}

	*data = math.Float64frombits(order.Uint64(bs))
	return nil
}

// read full is a bufio.Reader specific implementation of io.ReadFull() which
// avoids escaping our stack allocated scratch bytes
func readBuffFull(r *bufio.Reader, buf []byte) (n int, err error) {
	min := len(buf)
	for n < min && err == nil {
		var nn int
		nn, err = r.Read(buf[n:])
		n += nn
	}
	if n >= min {
		err = nil
	} else if n > 0 && err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return
}

// read full is a bytes.Buffer specific implementation of io.ReadFull() which
// avoids escaping our stack allocated scratch bytes
func readFull(r *bytes.Buffer, buf []byte) (n int, err error) {
	min := len(buf)
	for n < min && err == nil {
		var nn int
		nn, err = r.Read(buf[n:])
		n += nn
	}
	if n >= min {
		err = nil
	} else if n > 0 && err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return
}

func writeBool(w *bytes.Buffer, data bool) error {
	if data {
		w.WriteByte(1)
		return nil
	}

	w.WriteByte(0)
	return nil
}

func writeInt8(w *bytes.Buffer, data int8) error {
	w.WriteByte(byte(data))
	return nil
}

func writeUint8(w *bytes.Buffer, data uint8) error {
	w.WriteByte(byte(data))
	return nil
}

func writeInt16(w *bytes.Buffer, data int16) error {
	var b [2]byte
	bs := b[:]

	binary.LittleEndian.PutUint16(bs, uint16(data))
	_, err := w.Write(bs)
	return err
}

func writeUint16(w *bytes.Buffer, data uint16) error {
	var b [2]byte
	bs := b[:]

	binary.LittleEndian.PutUint16(bs, data)
	_, err := w.Write(bs)
	return err
}

func writeInt32(w *bytes.Buffer, data int32) error {
	var b [4]byte
	bs := b[:]

	binary.LittleEndian.PutUint32(bs, uint32(data))
	_, err := w.Write(bs)
	return err
}

func writeUint32(w *bytes.Buffer, data uint32) error {
	var b [4]byte
	bs := b[:]

	binary.LittleEndian.PutUint32(bs, data)
	_, err := w.Write(bs)
	return err
}

func writeInt(w *bytes.Buffer, data int) error {
	var b [4]byte
	bs := b[:]

	binary.LittleEndian.PutUint32(bs, uint32(int32(data)))
	_, err := w.Write(bs)
	return err
}

func writeUint(w *bytes.Buffer, data uint) error {
	var b [4]byte
	bs := b[:]

	binary.LittleEndian.PutUint32(bs, uint32(data))
	_, err := w.Write(bs)
	return err
}

func writeInt64(w *bytes.Buffer, data int64) error {
	var b [8]byte
	bs := b[:]

	binary.LittleEndian.PutUint64(bs, uint64(data))
	_, err := w.Write(bs)
	return err
}

func writeUint64(w *bytes.Buffer, data uint64) error {
	var b [8]byte
	bs := b[:]

	binary.LittleEndian.PutUint64(bs, data)
	_, err := w.Write(bs)
	return err
}

func writeFloat32(w *bytes.Buffer, data float32) error {
	var b [4]byte
	bs := b[:]

	binary.LittleEndian.PutUint32(bs, math.Float32bits(data))
	_, err := w.Write(bs)
	return err
}

func writeFloat64(w *bytes.Buffer, data float64) error {
	var b [8]byte
	bs := b[:]

	binary.LittleEndian.PutUint64(bs, math.Float64bits(data))
	_, err := w.Write(bs)
	return err
}
