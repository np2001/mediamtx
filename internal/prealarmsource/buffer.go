package prealarmsource

import (
	"time"

	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/mediamtx/internal/unit"
	"github.com/pion/rtp"
)

type BufferedUnit struct {
	Seq        uint64
	ReceivedAt time.Time
	Media      *description.Media
	Format     format.Format
	Unit       *unit.Unit
	KeyFrame   bool
}

type Cursor struct {
	LastSeq uint64
	Started bool
}

func (c *Cursor) ReadyItems(items []BufferedUnit, target time.Time) []BufferedUnit {
	var ret []BufferedUnit

	for _, item := range items {
		if item.Seq <= c.LastSeq {
			continue
		}

		if item.ReceivedAt.After(target) {
			break
		}

		if !c.Started && item.Media.Type == description.MediaTypeVideo {
			if !item.KeyFrame {
				c.LastSeq = item.Seq
				continue
			}
			c.Started = true
		}

		ret = append(ret, item)
		c.LastSeq = item.Seq
	}

	return ret
}

func cloneUnit(in *unit.Unit) *unit.Unit {
	if in == nil {
		return nil
	}

	out := *in
	out.RTPPackets = cloneRTPPackets(in.RTPPackets)
	out.Payload = clonePayload(in.Payload)

	return &out
}

func cloneRTPPackets(in []*rtp.Packet) []*rtp.Packet {
	if in == nil {
		return nil
	}

	out := make([]*rtp.Packet, len(in))

	for i, pkt := range in {
		if pkt == nil {
			continue
		}

		raw, err := pkt.Marshal()
		if err != nil {
			cp := *pkt
			cp.Payload = append([]byte(nil), pkt.Payload...)
			out[i] = &cp
			continue
		}

		var cp rtp.Packet
		err = cp.Unmarshal(raw)
		if err != nil {
			fallback := *pkt
			fallback.Payload = append([]byte(nil), pkt.Payload...)
			out[i] = &fallback
			continue
		}

		out[i] = &cp
	}

	return out
}

func clonePayload(in unit.Payload) unit.Payload {
	switch payload := in.(type) {
	case nil:
		return nil

	case unit.PayloadAC3:
		return unit.PayloadAC3(cloneBytes2(payload))

	case unit.PayloadAV1:
		return unit.PayloadAV1(cloneBytes2(payload))

	case unit.PayloadG711:
		return unit.PayloadG711(cloneBytes(payload))

	case unit.PayloadH264:
		return unit.PayloadH264(cloneBytes2(payload))

	case unit.PayloadH265:
		return unit.PayloadH265(cloneBytes2(payload))

	case unit.PayloadKLV:
		return unit.PayloadKLV(cloneBytes(payload))

	case unit.PayloadLPCM:
		return unit.PayloadLPCM(cloneBytes(payload))

	case unit.PayloadMJPEG:
		return unit.PayloadMJPEG(cloneBytes(payload))

	case unit.PayloadMPEG1Audio:
		return unit.PayloadMPEG1Audio(cloneBytes2(payload))

	case unit.PayloadMPEG1Video:
		return unit.PayloadMPEG1Video(cloneBytes(payload))

	case unit.PayloadMPEG4Audio:
		return unit.PayloadMPEG4Audio(cloneBytes2(payload))

	case unit.PayloadMPEG4AudioLATM:
		return unit.PayloadMPEG4AudioLATM(cloneBytes(payload))

	case unit.PayloadMPEG4Video:
		return unit.PayloadMPEG4Video(cloneBytes(payload))

	case unit.PayloadOpus:
		return unit.PayloadOpus(cloneBytes2(payload))

	case unit.PayloadVP8:
		return unit.PayloadVP8(cloneBytes(payload))

	case unit.PayloadVP9:
		return unit.PayloadVP9(cloneBytes(payload))

	default:
		return in
	}
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	return append([]byte(nil), in...)
}

func cloneBytes2(in [][]byte) [][]byte {
	if in == nil {
		return nil
	}

	out := make([][]byte, len(in))
	for i := range in {
		out[i] = cloneBytes(in[i])
	}
	return out
}

func isKeyFrame(medi *description.Media, forma format.Format, u *unit.Unit) bool {
	if medi == nil || medi.Type != description.MediaTypeVideo || u == nil {
		return true
	}

	switch forma.(type) {
	case *format.H264:
		return isH264KeyFrame(u)

	case *format.H265:
		return isH265KeyFrame(u)

	default:
		return true
	}
}

func isH264KeyFrame(u *unit.Unit) bool {
	payload, ok := u.Payload.(unit.PayloadH264)
	if !ok {
		return false
	}

	for _, nalu := range payload {
		if len(nalu) == 0 {
			continue
		}

		if nalu[0]&0x1F == 5 {
			return true
		}
	}

	return false
}

func isH265KeyFrame(u *unit.Unit) bool {
	payload, ok := u.Payload.(unit.PayloadH265)
	if !ok {
		return false
	}

	for _, nalu := range payload {
		if len(nalu) < 2 {
			continue
		}

		nalType := (nalu[0] >> 1) & 0x3F
		if nalType == 19 || nalType == 20 || nalType == 21 {
			return true
		}
	}

	return false
}
