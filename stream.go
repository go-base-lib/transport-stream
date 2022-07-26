package transport_stream

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gogo/protobuf/proto"
)

type Stream struct {
	rw *bufio.ReadWriter
}

var StreamIsEnd = errors.New("本次消息已到达尾端")

type MsgFlag byte

const (
	MsgFlagErr MsgFlag = iota
	MsgFlagSuccess
	MsgFlagEnd
)

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
	if marshal, e := err.Marshal(); err != nil {
		return fmt.Errorf("序列化异常信息失败: %s", e.Error())
	} else {
		return s.WriteMsg(marshal, MsgFlagErr)
	}
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
		return fmt.Errorf("向对端发送数据长度失败: %s", err.Error())
	}
	if err = s.rw.WriteByte(byte(flag)); err != nil {
		return fmt.Errorf("向对端发送成功标识失败: %s", err.Error())
	}
	if _, err = s.rw.Write(data); err != nil {
		return fmt.Errorf("向对端发送数据内容失败: %s", err.Error())
	}

	if err = s.rw.Flush(); err != nil {
		return fmt.Errorf("数据通道缓存刷新失败: %s", err.Error())
	}
	return nil
}

func (s *Stream) ReceiveMsg() ([]byte, error) {
	lenBuf, err := receiveBytesByLen(8, s.rw.Reader)
	if err != nil {
		return nil, fmt.Errorf("读取数据长度失败: %s", err.Error())
	}

	dataLen, err := BytesToInt[int64](lenBuf)
	if err != nil {
		return nil, fmt.Errorf("转换数据长度失败: %s", err.Error())
	}

	data, err := receiveBytesByLen(dataLen, s.rw.Reader)
	if err != nil {
		return nil, fmt.Errorf("获取数据内容失败: %s", err.Error())
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
