package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/relayra/relayra/internal/config"
	"github.com/relayra/relayra/internal/models"
	"github.com/relayra/relayra/internal/store"
)

// PeerDetailView shows details of a single peer with management options.
type PeerDetailView struct {
	cfg           *config.Config
	rdb           store.Backend
	peer          *models.Peer
	peerID        string
	isListener    bool
	err           error
	ready         bool
	actionIdx     int
	actions       []string
	confirm       bool
	confirmAction string
	deleted       bool
	queueSize     int64
	statusMsg     string
}

type peerDetailMsg struct {
	peer      *models.Peer
	queueSize int64
	err       error
}

type peerDeletedMsg struct {
	err error
}

type peerQueueClearedMsg struct {
	cleared int64
	err     error
}

// NewPeerDetailView creates a detail view for a specific peer.
func NewPeerDetailView(cfg *config.Config, rdb store.Backend, peerID string, isListener bool) *PeerDetailView {
	actions := []string{"Refresh"}
	if cfg.Role == config.RoleListener && !isListener {
		actions = []string{"Refresh", "Clear Queue", "Delete Peer"}
	}

	return &PeerDetailView{
		cfg:        cfg,
		rdb:        rdb,
		peerID:     peerID,
		isListener: isListener,
		actions:    actions,
	}
}

func (pd *PeerDetailView) Init() tea.Cmd {
	return pd.loadPeer
}

func (pd *PeerDetailView) loadPeer() tea.Msg {
	ctx := context.Background()
	var (
		peer *models.Peer
		err  error
	)
	if pd.isListener {
		peer, err = pd.rdb.GetListenerInfo(ctx)
	} else {
		peer, err = pd.rdb.GetPeer(ctx, pd.peerID)
	}
	if err != nil {
		return peerDetailMsg{err: err}
	}

	var queueSize int64
	if pd.isListener {
		queueSize, _ = pd.rdb.PendingResultsCount(ctx)
	} else {
		queueSize, _ = pd.rdb.QueueLength(ctx, pd.peerID)
	}

	return peerDetailMsg{peer: peer, queueSize: queueSize}
}

func (pd *PeerDetailView) deletePeer() tea.Msg {
	if pd.isListener {
		return peerDeletedMsg{}
	}
	err := pd.rdb.DeletePeer(context.Background(), pd.peerID)
	return peerDeletedMsg{err: err}
}

func (pd *PeerDetailView) clearQueue() tea.Msg {
	cleared, err := pd.rdb.ClearPeerQueue(context.Background(), pd.peerID)
	return peerQueueClearedMsg{cleared: cleared, err: err}
}

func (pd *PeerDetailView) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case peerDetailMsg:
		pd.peer = msg.peer
		pd.queueSize = msg.queueSize
		pd.err = msg.err
		pd.ready = true
		return pd, nil

	case peerDeletedMsg:
		if msg.err != nil {
			pd.err = msg.err
		} else {
			pd.deleted = true
		}
		return pd, nil

	case peerQueueClearedMsg:
		if msg.err != nil {
			pd.err = msg.err
		} else {
			pd.statusMsg = fmt.Sprintf("Cleared %d queued request(s).", msg.cleared)
			pd.ready = false
			return pd, pd.loadPeer
		}
		return pd, nil

	case tea.KeyMsg:
		if pd.confirm {
			switch msg.String() {
			case "y", "Y":
				action := pd.confirmAction
				pd.confirm = false
				pd.confirmAction = ""
				switch action {
				case "delete":
					return pd, pd.deletePeer
				case "clear":
					return pd, pd.clearQueue
				default:
					return pd, nil
				}
			default:
				pd.confirm = false
				pd.confirmAction = ""
				return pd, nil
			}
		}

		switch msg.String() {
		case "up", "k":
			if pd.actionIdx > 0 {
				pd.actionIdx--
			}
		case "down", "j":
			if pd.actionIdx < len(pd.actions)-1 {
				pd.actionIdx++
			}
		case "enter":
			return pd.executeAction()
		}
	}

	return pd, nil
}

func (pd *PeerDetailView) executeAction() (tea.Model, tea.Cmd) {
	switch pd.actions[pd.actionIdx] {
	case "Refresh":
		pd.ready = false
		pd.statusMsg = ""
		return pd, pd.loadPeer
	case "Clear Queue":
		pd.confirm = true
		pd.confirmAction = "clear"
	case "Delete Peer":
		pd.confirm = true
		pd.confirmAction = "delete"
	}
	return pd, nil
}

func (pd *PeerDetailView) View() string {
	var b strings.Builder

	b.WriteString(subtitleStyle.Render("  Peer Detail"))
	b.WriteString("\n\n")

	if !pd.ready {
		b.WriteString(dimStyle.Render("  Loading..."))
		return b.String()
	}

	if pd.deleted {
		b.WriteString(successStyle.Render("  Peer deleted successfully."))
		b.WriteString("\n\n")
		b.WriteString(dimStyle.Render("  esc back to peers"))
		return b.String()
	}

	if pd.err != nil {
		b.WriteString(errorStyle.Render(fmt.Sprintf("  Error: %v", pd.err)))
		b.WriteString("\n\n")
		b.WriteString(dimStyle.Render("  esc back"))
		return b.String()
	}

	if pd.peer == nil {
		b.WriteString(errorStyle.Render("  Peer not found"))
		b.WriteString("\n\n")
		b.WriteString(dimStyle.Render("  esc back"))
		return b.String()
	}

	p := pd.peer

	b.WriteString(fmt.Sprintf("  %s %s\n", labelStyle.Render("Name:"), valueStyle.Render(p.Name)))
	b.WriteString(fmt.Sprintf("  %s %s\n", labelStyle.Render("Peer ID:"), valueStyle.Render(p.ID)))
	b.WriteString(fmt.Sprintf("  %s %s\n", labelStyle.Render("Machine ID:"), valueStyle.Render(p.MachineID)))
	b.WriteString(fmt.Sprintf("  %s %s\n", labelStyle.Render("Role:"), valueStyle.Render(p.Role)))
	if p.Address != "" {
		b.WriteString(fmt.Sprintf("  %s %s\n", labelStyle.Render("Address:"), valueStyle.Render(p.Address)))
	}
	if len(p.Capabilities) > 0 {
		b.WriteString(fmt.Sprintf("  %s %s\n", labelStyle.Render("Capabilities:"), valueStyle.Render(strings.Join(p.Capabilities, ", "))))
	}
	b.WriteString(fmt.Sprintf("  %s %s\n", labelStyle.Render("Registered:"), valueStyle.Render(p.RegisteredAt.Format("2006-01-02 15:04:05"))))

	lastSeenStr := p.LastSeen.Format("2006-01-02 15:04:05")
	age := time.Since(p.LastSeen)
	ageStyle := successStyle
	switch {
	case age > time.Hour:
		ageStyle = errorStyle
		lastSeenStr += fmt.Sprintf(" (%s ago)", formatDuration(age))
	case age > 10*time.Minute:
		ageStyle = warnStyle
		lastSeenStr += fmt.Sprintf(" (%s ago)", formatDuration(age))
	default:
		lastSeenStr += " (active)"
	}
	b.WriteString(fmt.Sprintf("  %s %s\n", labelStyle.Render("Last Seen:"), ageStyle.Render(lastSeenStr)))

	queueLabel := "Queue Size:"
	if pd.isListener && pd.cfg.Role == config.RoleSender {
		queueLabel = "Pending Results:"
	}
	b.WriteString(fmt.Sprintf("  %s %s\n", labelStyle.Render(queueLabel), valueStyle.Render(fmt.Sprintf("%d", pd.queueSize))))

	if pd.statusMsg != "" {
		b.WriteString("\n")
		b.WriteString(successStyle.Render("  " + pd.statusMsg))
	}

	b.WriteString("\n\n")

	for i, action := range pd.actions {
		cursor := "  "
		style := normalStyle
		if i == pd.actionIdx {
			cursor = "> "
			style = selectedStyle
		}
		b.WriteString(style.Render(cursor + action))
		b.WriteString("\n")
	}

	if pd.confirm {
		b.WriteString("\n")
		switch pd.confirmAction {
		case "clear":
			b.WriteString(warnStyle.Render("  Clear queued requests for this peer? (y/n)"))
		case "delete":
			b.WriteString(errorStyle.Render("  Delete this peer? (y/n)"))
		}
	}

	b.WriteString("\n")
	b.WriteString(dimStyle.Render("  up/down navigate • enter select • esc back"))

	return b.String()
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}
