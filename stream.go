package transport_stream

import (
	"bufio"
	"errors"
	"fmt"
)

type StreamMsg struct {
	Success bool
}

type Stream struct {
	rw *bufio.ReadWriter
}

func (s *Stream) WriteJsonMsg(msg any) error {
	return utils.WriteJsonMsgToWriter(s.rw.Writer, msg)
}

func (s *Stream) ReceiveJsonMsg(msg any) error {
	return utils.ReadJsonMsgByReader(s.rw.Reader, msg)
}

func (s *Stream) WriteProtoMsg(msg proto.Message) error {
	return utils.WriteProtoMsgToWriter(s.rw.Writer, msg)
}

func (s *Stream) ReceiveProtoMsg(msg proto.Message) error {
	return utils.ReadProtoMsgByReader(s.rw.Reader, msg)
}

func (s *Stream) WriteError(err *errors.Error) error {
	return utils.WriteErrToWriter(s.rw.Writer, err)
}

func (s *Stream) WriteMsg(data []byte) error {
	return utils.WriteBytesToWriter(s.rw.Writer, data, true)
}

func (s *Stream) ReceiveMsg() (res *StreamMsg, err error) {
	lenBuf, err := s.receiveBytesByLen(8)
	if err != nil {
		return nil, fmt.Errorf("读取数据长度失败: %s", err.Error())
	}

}

func (s *Stream) receiveBytesByLen(l int) ([]byte, error) {
	var (
		b byte

		res []byte
		err error
	)
	res = make([]byte, l)
	for i := 0; i < l; i++ {
		if b, err = s.rw.ReadByte(); err != nil {
			return nil, err
		}
		res[i] = b
	}
	return res, nil
}

func NewStream(stream quic.Stream) *Stream {
	return &Stream{
		rw: bufio.NewReadWriter(bufio.NewReader(stream), bufio.NewWriter(stream)),
	}
}
