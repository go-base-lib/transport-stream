package transport_stream

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"io"
	"net"
)

func selectIoEofErr(err error, otherStr string, args ...any) error {
	if err == io.EOF {
		return err
	}
	return fmt.Errorf(otherStr, args)
}

type Stream struct {
	rw  *bufio.ReadWriter
	err error
}

var StreamIsEnd = errors.New("本次消息已到达尾端")

type MsgFlag byte

const (
	MsgFlagErr MsgFlag = iota
	MsgFlagSuccess
	MsgFlagEnd
)

func (s *Stream) errWrapper(fn func() error) *Stream {
	if s.err != nil {
		return s
	}
	s.err = fn()
	return s
}

func (s *Stream) Error() error {
	return s.err
}

func (s *Stream) WriteProtoMsgStream(msg proto.Message) *Stream {
	return s.errWrapper(func() error {
		return s.WriteProtoMsg(msg)
	})
}

func (s *Stream) WriteJsonMsgStream(msg any) *Stream {
	return s.errWrapper(func() error {
		return s.WriteJsonMsg(msg)
	})
}

func (s *Stream) WriteErrorStream(err *ErrInfo) *Stream {
	return s.errWrapper(func() error {
		return s.WriteError(err)
	})
}

func (s *Stream) WriteMsgStream(data []byte, flag MsgFlag) *Stream {
	return s.errWrapper(func() error {
		return s.WriteMsg(data, flag)
	})
}

func (s *Stream) WriteJsonMsg(msg any) error {
	if marshal, err := json.Marshal(msg); err != nil {
		return fmt.Errorf("序列化JSON数据失败: %s", err.Error())
	} else {
		return s.WriteMsg(marshal, MsgFlagSuccess)
	}
}

func (s *Stream) ReceiveJsonMsg(msg any) error {
	receiveMsg, err := s.ReceiveMsg()
	if err != nil {
		return err
	}
	if err = json.Unmarshal(receiveMsg, msg); err != nil {
		return fmt.Errorf("反序列化数据到JSON失败: %s", err.Error())
	}
	return nil
}

func (s *Stream) WriteProtoMsg(msg proto.Message) error {
	if marshal, err := proto.Marshal(msg); err != nil {
		return fmt.Errorf("序列化proto数据失败: %s", err.Error())
	} else {
		return s.WriteMsg(marshal, MsgFlagSuccess)
	}
}

func (s *Stream) ReceiveProtoMsg(msg proto.Message) error {
	receiveMsg, err := s.ReceiveMsg()
	if err != nil {
		return err
	}
	if err = proto.Unmarshal(receiveMsg, msg); err != nil {
		return fmt.Errorf("反序列化数据到proto失败: %s", err.Error())
	}
	return err
}

func (s *Stream) WriteError(err *ErrInfo) error {
	if marshal, e := err.Marshal(); e != nil {
		return fmt.Errorf("序列化异常信息失败: %s", e.Error())
	} else {
		return s.WriteMsg(marshal, MsgFlagErr)
	}
}

func (s *Stream) WriteEndMsg() error {
	return s.WriteEndMsgWithData(nil)
}

func (s *Stream) WriteEndMsgWithData(data []byte) error {
	return s.WriteMsg(data, MsgFlagEnd)
}

// WriteMsg 向对端通道写入数据, data 为写入的内容
// flag为内容标识, 0 为错误消息, 1 为正确消息, 非0和1代表一次消息的结束
func (s *Stream) WriteMsg(data []byte, flag MsgFlag) error {
	dataLen := int64(len(data) + 1)
	lenBytes, err := IntToBytes(dataLen)
	if err != nil {
		return fmt.Errorf("转换数据长度失败: %s", err.Error())
	}
	if _, err = s.rw.Write(lenBytes); err != nil {
		return selectIoEofErr(err, "向对端发送数据长度失败: %s", err.Error())
	}
	if err = s.rw.WriteByte(byte(flag)); err != nil {
		return selectIoEofErr(err, "向对端发送成功标识失败: %s", err.Error())
	}
	if _, err = s.rw.Write(data); err != nil {
		return selectIoEofErr(err, "向对端发送数据内容失败: %s", err.Error())
	}

	if err = s.rw.Flush(); err != nil {
		return selectIoEofErr(err, "数据通道缓存刷新失败: %s", err.Error())
	}
	return nil
}

func (s *Stream) ReceiveMsg() ([]byte, error) {
	lenBuf, err := receiveBytesByLen(8, s.rw.Reader)
	if err != nil {
		return nil, selectIoEofErr(err, "读取数据长度失败: %s", err.Error())
	}

	dataLen, err := BytesToInt[int64](lenBuf)
	if err != nil {
		return nil, fmt.Errorf("转换数据长度失败: %s", err.Error())
	}

	data, err := receiveBytesByLen(dataLen, s.rw.Reader)
	if err != nil {
		return nil, selectIoEofErr(err, "获取数据内容失败: %s", err.Error())
	}

	msgFlag := MsgFlag(data[0])
	otherData := data[1:]
	switch msgFlag {
	case MsgFlagSuccess:
		return otherData, nil
	case MsgFlagErr:
		var errInfo *ErrInfo
		if err = json.Unmarshal(otherData, &errInfo); err != nil {
			return otherData, fmt.Errorf("对端返回错误, 但解析错误内容失败: %s", err.Error())
		}
		return nil, errInfo
	default:
		return otherData, StreamIsEnd
	}
}

func receiveBytesByLen[T IntType](l T, r *bufio.Reader) ([]byte, error) {
	var (
		b byte

		res []byte
		err error
	)
	res = make([]byte, l)
	for i := T(0); i < l; i++ {
		if b, err = r.ReadByte(); err != nil {
			return nil, err
		}
		res[i] = b
	}
	return res, nil
}

func NewStream(rw *bufio.ReadWriter) *Stream {
	return &Stream{
		rw: rw,
	}
}

func NewStreamByConn(conn net.Conn) *Stream {
	return NewStream(bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)))
}
