package transport_stream

import (
	"encoding/json"
	"fmt"
	"io"
)

type ErrCode uint

func (e ErrCode) New(msg string) *ErrInfo {
	return &ErrInfo{
		Code: e,
		Msg:  msg,
	}
}

func (e ErrCode) Newf(msg string, args ...any) *ErrInfo {
	return e.New(fmt.Sprintf(msg, args...))
}

func (e ErrCode) NewWithData(msg string, data any) (*ErrInfo, error) {
	marshal, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("序列化数据失败: %s", err.Error())
	}
	return &ErrInfo{
		Code:    e,
		Msg:     msg,
		RawData: marshal,
	}, nil
}

func (e ErrCode) NewWithDataf(data any, msg string, args ...any) (*ErrInfo, error) {
	return e.NewWithData(fmt.Sprintf(msg, args...), data)
}

func (e ErrCode) Equal(err error) bool {
	_err, b := ErrConvert(err)
	if !b {
		return false
	}
	return _err.Code == e
}

type ErrInfo struct {
	Code    ErrCode
	Msg     string
	RawData []byte
}

func (e *ErrInfo) Error() string {
	return e.Msg
}

func (e *ErrInfo) Marshal() ([]byte, error) {
	var (
		err error
		res []byte
	)
	res, err = json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("序列化异常数据包失败: %s", err.Error())
	}
	return res, nil
}

func (e *ErrInfo) UnmarshalData(i any) error {
	if e.RawData == nil {
		return io.EOF
	}

	if err := json.Unmarshal(e.RawData, i); err != nil {
		return fmt.Errorf("转换错误中的数据失败: %s", err.Error())
	}

	return nil
}

func (e *ErrInfo) WriteTo(stream *Stream) error {
	return stream.WriteError(e)
}

func ErrConvert(e error) (*ErrInfo, bool) {
	err, ok := e.(*ErrInfo)
	return err, ok
}
