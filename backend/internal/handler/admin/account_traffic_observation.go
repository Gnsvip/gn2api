package admin

import (
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
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
	view := service.GetOpenAITrafficObservation(id, time.Duration(minutes)*time.Minute)
	protection := service.OpenAITrafficGuardView(account)
	view.Protection = &protection
	if protection.Config.Enabled {
		view.Mode = "observe_and_protect"
	}
	response.Success(c, view)
}

func (h *AccountHandler) SetTrafficGuard(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		response.BadRequest(c, "Invalid account ID")
		return
	}
	var input struct {
		Enabled         *bool `json:"enabled"`
		CooldownMinutes int   `json:"cooldown_minutes"`
	}
	if c.ShouldBindJSON(&input) != nil || input.Enabled == nil || input.CooldownMinutes < 1 || input.CooldownMinutes > 1440 {
		response.BadRequest(c, "须提供开关，冷却时间须为 1–1440 分钟")
		return
	}
	account, err := h.adminService.GetAccount(c.Request.Context(), id)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	if account == nil || !account.IsOpenAI() {
		response.BadRequest(c, "性能冷却目前支持 OpenAI 账号")
		return
	}
	cfg := service.OpenAITrafficGuardConfig(account)
	if cfg.Revision == "" || cfg.Enabled != *input.Enabled || cfg.CooldownMinutes != input.CooldownMinutes {
		cfg = service.TrafficGuardConfig{Enabled: *input.Enabled, CooldownMinutes: input.CooldownMinutes, Revision: uuid.NewString(), Since: time.Now().UTC()}
		if err := h.adminService.UpdateAccountExtra(c.Request.Context(), id, map[string]any{service.TrafficGuardExtraKey: cfg}); err != nil {
			response.ErrorFrom(c, err)
			return
		}
	}
	response.Success(c, cfg)
}
