package errorx

import (
	"fmt"

	logger "github.com/2997215859/golog"
	"github.com/samber/lo"
)

type Error struct {
	Code int
	Msg  string
}

func (e *Error) Error() string {
	return fmt.Sprintf("Code=%d Msg=%s", e.Code, e.Msg)
}

func NewError(format string, v ...interface{}) error {
	err := fmt.Errorf(format, v...)
	logger.Logger.WithField(logger.SkipKey, 1).Error(err.Error())
	return err
}

func NewBizError(code int, format string, v ...interface{}) error {
	err := &Error{
		Code: code,
		Msg:  fmt.Sprintf(format, v...),
	}

	logger.Logger.WithField(logger.SkipKey, 1).Error(err.Error())
	return err
}

var LoginError = fmt.Errorf("plz login first")

func GetBizCode(err error) int {
	if err == nil {
		return CodeOK
	}
	xerr, ok := lo.ErrorsAs[*Error](err)
	if ok {
		return xerr.Code
	}
	return CodeCommon
}

const (
	CodeOK                  = 0
	CodeCommon              = 10000000
	CodeInvalidParam        = 10000001
	CodeUpstreamApiNotStart = 10000002
)
