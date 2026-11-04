package admin

import (
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func (h *AccountHandler) GetTrafficObservation(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		response.BadRequest(c, "Invalid account ID")
		return
	}
	minutes, err := strconv.Atoi(c.DefaultQuery("window_minutes", "60"))
	if err != nil || (minutes != 60 && minutes != 360 && minutes != 1440) {
		response.BadRequest(c, "观察窗口须为 60、360 或 1440 分钟")
		return
	}
	account, err := h.adminService.GetAccount(c.Request.Context(), id)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	if account == nil || !account.IsOpenAI() {
		response.BadRequest(c, "首字观察目前支持 OpenAI 账号")
		return
	}
	response.Success(c, service.GetOpenAITrafficObservation(id, time.Duration(minutes)*time.Minute))
}
