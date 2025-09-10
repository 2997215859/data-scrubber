package main

import (
	"data-scrubber/biz/errorx"
	"data-scrubber/biz/service"
	"data-scrubber/biz/upstream/gotushare"
	logger "github.com/2997215859/golog"
	"github.com/gocarina/gocsv"
	"os"
	"testing"
)

type TradeCalendarItem struct {
	Exchange     string `json:"exchange,omitempty"`      // str Y	交易所 SSE上交所 SZSE深交所
	CalDate      string `json:"cal_date,omitempty"`      // str Y	日历日期
	IsOpen       int    `json:"is_open,omitempty"`       // str Y	是否交易 0休市 1交易
	PretradeDate string `json:"pretrade_date,omitempty"` // str N	上一个交易日
}

func GetTradeCalendar(exchange string, startDate, endDate string) ([]*TradeCalendarItem, error) {
	r := gotushare.TradeCalRequest{
		Exchange:  exchange,
		StartDate: startDate,
		EndDate:   endDate,
	}

	rsp, err := service.GetTuShare().TradeCal(r, gotushare.TradeCalItems{}.All())
	if err != nil {
		return nil, errorx.NewError("TradeCal err: %v", err)
	}
	if rsp.Code != 0 {
		return nil, errorx.NewError("TradeCal code(%+v) != 0. msg = %s", rsp.Code, rsp.Msg)
	}

	res := make([]*TradeCalendarItem, 0)
	for _, v := range rsp.Data.Items {
		res = append(res, &TradeCalendarItem{
			Exchange:     v[0].(string),
			CalDate:      v[1].(string),
			IsOpen:       int(v[2].(float64)),
			PretradeDate: v[3].(string),
		})
	}
	return res, nil
}

func TestTradeCalendar(t *testing.T) {
	service.InitTuShare()
	res1, nil := GetTradeCalendar("SSE", "20250101", "20251231")
	res2, nil := GetTradeCalendar("SZSE", "20250101", "20251231")

	res1 = append(res1, res2...)

	filename := "/Users/bytedance/workspace/code/data-scrubber/testdata/trade_calendar.csv"

	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		logger.Error("Failed to open file(%s): %v", filename, err)
		return
	}
	defer file.Close()

	err = gocsv.MarshalFile(&res1, file) // Use this to save the CSV back to the file
	if err != nil {
		logger.Error("Failed to Marshal file: %v", err)
		return
	}

}
