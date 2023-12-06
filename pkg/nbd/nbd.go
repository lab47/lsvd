package nbd

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"

	"github.com/hashicorp/go-hclog"
	"github.com/pkg/errors"
)

var (
	ErrInvalidMagic     = errors.New("invalid magic")
	ErrInvalidBlocksize = errors.New("invalid blocksize")
)

const (
	defaultMaximumRequestSize = 32 * 1024 * 1024 // Support for a 32M maximum packet size is expected: https://sourceforge.net/p/nbd/mailman/message/35081223/
)

type Export struct {
	Name        string
	Description string

	Backend Backend
}

type Options struct {
	ReadOnly bool

	MinimumBlockSize   uint32
	PreferredBlockSize uint32
	MaximumBlockSize   uint32

	MaximumRequestSize int
	SupportsMultiConn  bool
}

func Handle(log hclog.Logger, conn net.Conn, exports []*Export, options *Options) error {
	if options == nil {
		options = &Options{
			ReadOnly:          false,
			SupportsMultiConn: true,
		}
	}

	if options.MinimumBlockSize == 0 {
		options.MinimumBlockSize = 1
	}

	if options.PreferredBlockSize == 0 {
		options.PreferredBlockSize = 4096
	}

	if options.MaximumBlockSize == 0 {
		options.MaximumBlockSize = defaultMaximumRequestSize
	}

	if options.MaximumRequestSize == 0 {
		options.MaximumRequestSize = defaultMaximumRequestSize
	}

	// Negotiation
	if err := binary.Write(conn, binary.BigEndian, NegotiationNewstyleHeader{
		OldstyleMagic:  NEGOTIATION_MAGIC_OLDSTYLE,
		OptionMagic:    NEGOTIATION_MAGIC_OPTION,
		HandshakeFlags: NEGOTIATION_HANDSHAKE_FLAG_FIXED_NEWSTYLE,
	}); err != nil {
		return errors.Wrapf(err, "unable to negation newstyle header")
	}

	var clientFlags uint32

	err := binary.Read(conn, binary.BigEndian, &clientFlags)
	if err != nil {
		return err
	}

	log.Trace("client flags", "value", clientFlags)

	log.Trace("sent negotation header, reading options")

	var export *Export

nego:
	for {
		var optionHeader NegotiationOptionHeader
		if err := binary.Read(conn, binary.BigEndian, &optionHeader); err != nil {
			return errors.Wrapf(err, "reading negation option")
		}

		if optionHeader.OptionMagic != NEGOTIATION_MAGIC_OPTION {
			return ErrInvalidMagic
		}

		log.Trace("negoation option", "id", optionHeader.ID, "len", optionHeader.Length)

		switch optionHeader.ID {
		case NEGOTIATION_ID_OPTION_INFO, NEGOTIATION_ID_OPTION_GO:
			var exportNameLength uint32
			if err := binary.Read(conn, binary.BigEndian, &exportNameLength); err != nil {
				return err
			}

			exportName := make([]byte, exportNameLength)
			if _, err := io.ReadFull(conn, exportName); err != nil {
				return err
			}

			log.Debug("looking for export", "name", exportName)

			for _, candidate := range exports {
				if candidate.Name == string(exportName) {
					export = candidate

					break
				}
			}

			if export == nil {
				log.Error("no export found", "name", exportName)

				if length := int64(optionHeader.Length) - 4 - int64(exportNameLength); length > 0 { // Discard the option's data, minus the export name length and export name we've already read
					_, err := io.CopyN(io.Discard, conn, length)
					if err != nil {
						return err
					}
				}

				if err := binary.Write(conn, binary.BigEndian, NegotiationReplyHeader{
					ReplyMagic: NEGOTIATION_MAGIC_REPLY,
					ID:         optionHeader.ID,
					Type:       NEGOTIATION_TYPE_REPLY_ERR_UNKNOWN,
					Length:     0,
				}); err != nil {
					return err
				}

				break
			}

			size, err := export.Backend.Size()
			if err != nil {
				return err
			}

			{
				var informationRequestCount uint16
				if err := binary.Read(conn, binary.BigEndian, &informationRequestCount); err != nil {
					return err
				}

				_, err := io.CopyN(io.Discard, conn, 2*int64(informationRequestCount)) // Discard information requests (uint16s)
				if err != nil {
					return err
				}
			}

			log.Debug("reporting device size", "size", size)

			{
				transmissionFlags := uint16(0)
				if options.SupportsMultiConn {
					transmissionFlags = NEGOTIATION_REPLY_FLAGS_HAS_FLAGS | NEGOTIATION_REPLY_FLAGS_CAN_MULTI_CONN
				}

				info := &bytes.Buffer{}
				if err := binary.Write(info, binary.BigEndian, NegotiationReplyInfo{
					Type:              NEGOTIATION_TYPE_INFO_EXPORT,
					Size:              uint64(size),
					TransmissionFlags: transmissionFlags,
				}); err != nil {
					return err
				}

				if err := binary.Write(conn, binary.BigEndian, NegotiationReplyHeader{
					ReplyMagic: NEGOTIATION_MAGIC_REPLY,
					ID:         optionHeader.ID,
					Type:       NEGOTIATION_TYPE_REPLY_INFO,
					Length:     uint32(info.Len()),
				}); err != nil {
					return err
				}

				if _, err := io.Copy(conn, info); err != nil {
					return err
				}
			}

			{
				info := &bytes.Buffer{}
				if err := binary.Write(info, binary.BigEndian, NegotiationReplyNameHeader{
					Type: NEGOTIATION_TYPE_INFO_NAME,
				}); err != nil {
					return err
				}

				if _, err := info.Write([]byte(exportName)); err != nil {
					return err
				}

				if err := binary.Write(conn, binary.BigEndian, NegotiationReplyHeader{
					ReplyMagic: NEGOTIATION_MAGIC_REPLY,
					ID:         optionHeader.ID,
					Type:       NEGOTIATION_TYPE_REPLY_INFO,
					Length:     uint32(info.Len()),
				}); err != nil {
					return err
				}

				if _, err := io.Copy(conn, info); err != nil {
					return err
				}
			}

			{
				info := &bytes.Buffer{}
				if err := binary.Write(info, binary.BigEndian, NegotiationReplyDescriptionHeader{
					Type: NEGOTIATION_TYPE_INFO_DESCRIPTION,
				}); err != nil {
					return err
				}

				if err := binary.Write(info, binary.BigEndian, []byte(export.Description)); err != nil {
					return err
				}

				if err := binary.Write(conn, binary.BigEndian, NegotiationReplyHeader{
					ReplyMagic: NEGOTIATION_MAGIC_REPLY,
					ID:         optionHeader.ID,
					Type:       NEGOTIATION_TYPE_REPLY_INFO,
					Length:     uint32(info.Len()),
				}); err != nil {
					return err
				}

				if _, err := io.Copy(conn, info); err != nil {
					return err
				}
			}

			{
				info := &bytes.Buffer{}
				if err := binary.Write(info, binary.BigEndian, NegotiationReplyBlockSize{
					Type:               NEGOTIATION_TYPE_INFO_BLOCKSIZE,
					MinimumBlockSize:   options.MinimumBlockSize,
					PreferredBlockSize: options.PreferredBlockSize,
					MaximumBlockSize:   options.MaximumBlockSize,
				}); err != nil {
					return err
				}

				if err := binary.Write(conn, binary.BigEndian, NegotiationReplyHeader{
					ReplyMagic: NEGOTIATION_MAGIC_REPLY,
					ID:         optionHeader.ID,
					Type:       NEGOTIATION_TYPE_REPLY_INFO,
					Length:     uint32(info.Len()),
				}); err != nil {
					return err
				}

				if _, err := io.Copy(conn, info); err != nil {
					return err
				}
			}

			if err := binary.Write(conn, binary.BigEndian, NegotiationReplyHeader{
				ReplyMagic: NEGOTIATION_MAGIC_REPLY,
				ID:         optionHeader.ID,
				Type:       NEGOTIATION_TYPE_REPLY_ACK,
				Length:     0,
			}); err != nil {
				return err
			}

			log.Debug("entering transmission mode")

			if optionHeader.ID == NEGOTIATION_ID_OPTION_GO {
				break nego
			}
		case NEGOTIATION_ID_OPTION_ABORT:
			if err := binary.Write(conn, binary.BigEndian, NegotiationReplyHeader{
				ReplyMagic: NEGOTIATION_MAGIC_REPLY,
				ID:         optionHeader.ID,
				Type:       NEGOTIATION_TYPE_REPLY_ACK,
				Length:     0,
			}); err != nil {
				return err
			}

			return nil
		case NEGOTIATION_ID_OPTION_LIST:
			{
				info := &bytes.Buffer{}

				for _, export := range exports {
					exportName := []byte(export.Name)

					if err := binary.Write(info, binary.BigEndian, uint32(len(exportName))); err != nil {
						return err
					}

					if err := binary.Write(info, binary.BigEndian, exportName); err != nil {
						return err
					}
				}

				if err := binary.Write(conn, binary.BigEndian, NegotiationReplyHeader{
					ReplyMagic: NEGOTIATION_MAGIC_REPLY,
					ID:         optionHeader.ID,
					Type:       NEGOTIATION_TYPE_REPLY_SERVER,
					Length:     uint32(info.Len()),
				}); err != nil {
					return err
				}

				if _, err := io.Copy(conn, info); err != nil {
					return err
				}
			}

			if err := binary.Write(conn, binary.BigEndian, NegotiationReplyHeader{
				ReplyMagic: NEGOTIATION_MAGIC_REPLY,
				ID:         optionHeader.ID,
				Type:       NEGOTIATION_TYPE_REPLY_ACK,
				Length:     0,
			}); err != nil {
				return err
			}
		default:
			_, err := io.CopyN(io.Discard, conn, int64(optionHeader.Length)) // Discard the unknown option's data
			if err != nil {
				return err
			}

			if err := binary.Write(conn, binary.BigEndian, NegotiationReplyHeader{
				ReplyMagic: NEGOTIATION_MAGIC_REPLY,
				ID:         optionHeader.ID,
				Type:       NEGOTIATION_TYPE_REPLY_ERR_UNSUPPORTED,
				Length:     0,
			}); err != nil {
				return err
			}
		}
	}

	// Transmission
	b := []byte{}
	for {
		var requestHeader TransmissionRequestHeader
		if err := binary.Read(conn, binary.BigEndian, &requestHeader); err != nil {
			return err
		}

		if requestHeader.RequestMagic != TRANSMISSION_MAGIC_REQUEST {
			return ErrInvalidMagic
		}

		length := requestHeader.Length
		if length > defaultMaximumRequestSize {
			return ErrInvalidBlocksize
		}

		if length != uint32(len(b)) {
			b = make([]byte, length)
		}

		switch requestHeader.Type {
		case TRANSMISSION_TYPE_REQUEST_READ:
			if err := binary.Write(conn, binary.BigEndian, TransmissionReplyHeader{
				ReplyMagic: TRANSMISSION_MAGIC_REPLY,
				Error:      0,
				Handle:     requestHeader.Handle,
			}); err != nil {
				return err
			}

			n, err := export.Backend.ReadAt(b[:length], int64(requestHeader.Offset))
			if err != nil {
				return err
			}

			if _, err := conn.Write(b[:n]); err != nil {
				return err
			}
		case TRANSMISSION_TYPE_REQUEST_WRITE:
			if options.ReadOnly {
				_, err := io.CopyN(io.Discard, conn, int64(requestHeader.Length)) // Discard the write command's data
				if err != nil {
					return err
				}

				if err := binary.Write(conn, binary.BigEndian, TransmissionReplyHeader{
					ReplyMagic: TRANSMISSION_MAGIC_REPLY,
					Error:      TRANSMISSION_ERROR_EPERM,
					Handle:     requestHeader.Handle,
				}); err != nil {
					return err
				}

				break
			}

			n, err := io.ReadAtLeast(conn, b[:length], int(requestHeader.Length))
			if err != nil {
				return err
			}

			if _, err := export.Backend.WriteAt(b[:n], int64(requestHeader.Offset)); err != nil {
				return err
			}

			if err := binary.Write(conn, binary.BigEndian, TransmissionReplyHeader{
				ReplyMagic: TRANSMISSION_MAGIC_REPLY,
				Error:      0,
				Handle:     requestHeader.Handle,
			}); err != nil {
				return err
			}
		case TRANSMISSION_TYPE_REQUEST_DISC:
			if !options.ReadOnly {
				if err := export.Backend.Sync(); err != nil {
					return err
				}
			}

			return nil
		default:
			_, err := io.CopyN(io.Discard, conn, int64(requestHeader.Length)) // Discard the unknown command's data
			if err != nil {
				return err
			}

			if err := binary.Write(conn, binary.BigEndian, TransmissionReplyHeader{
				ReplyMagic: TRANSMISSION_MAGIC_REPLY,
				Error:      TRANSMISSION_ERROR_EINVAL,
				Handle:     requestHeader.Handle,
			}); err != nil {
				return err
			}
		}
	}
}