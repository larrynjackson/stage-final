package message

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
)

type (
	Transport struct {
		Length int
		Data   []byte
	}
)

func (t *Transport) String() string {
	return fmt.Sprintf("Length[%02d] Data[%s]", t.Length, t.Data)
}

func (t *Transport) Write(c net.Conn) error {
	data := make([]byte, 0, 4+t.Length)

	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(t.Length))
	data = append(data, buf...)

	w := bytes.Buffer{}
	err := binary.Write(&w, binary.BigEndian, t.Data)
	if err != nil {
		return err
	}

	data = append(data, w.Bytes()...)

	_, err = c.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func (t *Transport) Read(c net.Conn) error {
	buf := make([]byte, 4)

	_, err := c.Read(buf)
	if err != nil {
		return err
	}

	byteCount := binary.BigEndian.Uint32(buf)
	t.Length = int(byteCount)
	t.Data = make([]byte, t.Length)

	_, err = c.Read(t.Data)
	if err != nil {
		return err
	}

	return nil
}
