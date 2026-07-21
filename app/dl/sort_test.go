package dl

import (
	"testing"

	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/assert"

	"github.com/iyear/tdl/pkg/tmessage"
)

func dialog(channelID int64, msgs ...int) *tmessage.Dialog {
	return &tmessage.Dialog{
		Peer:     &tg.InputPeerChannel{ChannelID: channelID},
		Messages: msgs,
	}
}

func TestSortDialogs(t *testing.T) {
	t.Run("default sorts messages by id ascending", func(t *testing.T) {
		d := []*tmessage.Dialog{dialog(1, 30, 10, 20)}
		sortDialogs(d, false, false)
		assert.Equal(t, []int{10, 20, 30}, d[0].Messages)
	})

	t.Run("desc sorts messages by id descending", func(t *testing.T) {
		d := []*tmessage.Dialog{dialog(1, 10, 30, 20)}
		sortDialogs(d, true, false)
		assert.Equal(t, []int{30, 20, 10}, d[0].Messages)
	})

	t.Run("keepOrder preserves input message order", func(t *testing.T) {
		// caller ordered these by media size, not by id; must stay as given
		d := []*tmessage.Dialog{dialog(1, 30, 10, 20)}
		sortDialogs(d, false, true)
		assert.Equal(t, []int{30, 10, 20}, d[0].Messages)
	})

	t.Run("keepOrder still orders dialogs by peer for stable fingerprint", func(t *testing.T) {
		d := []*tmessage.Dialog{dialog(2, 5, 1), dialog(1, 9, 3)}
		sortDialogs(d, false, true)
		// dialogs sorted by peer id (1 before 2), messages untouched
		assert.Equal(t, int64(1), d[0].Peer.(*tg.InputPeerChannel).ChannelID)
		assert.Equal(t, []int{9, 3}, d[0].Messages)
		assert.Equal(t, int64(2), d[1].Peer.(*tg.InputPeerChannel).ChannelID)
		assert.Equal(t, []int{5, 1}, d[1].Messages)
	})
}
