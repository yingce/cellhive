package peer

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"cellhive/internal/cell"
)

// A real TCP peer can accept a connection without completing its protocol.
func stalledPeer(t *testing.T, upgrade, drain bool) (string, <-chan struct{}) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan struct{})
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := l.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		go func() { <-stop; c.Close() }()
		r := bufio.NewReader(c)
		if _, err = http.ReadRequest(r); err != nil {
			return
		}
		close(accepted)
		if upgrade {
			fmt.Fprintf(c, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: %s\r\n\r\n", StreamProtocol)
		}
		if drain {
			io.Copy(io.Discard, r)
		} else {
			<-stop
		}
	}()
	t.Cleanup(func() { close(stop); l.Close(); <-done })
	return "http://" + l.Addr().String(), accepted
}
func TestStreamHandshakeCancellation(t *testing.T) {
	url, _ := stalledPeer(t, false, false)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		s, e := dialRawStream(ctx, url, "", "", 1)
		if s != nil {
			s.fail(io.EOF)
		}
		done <- e
	}()
	select {
	case e := <-done:
		if e == nil {
			t.Fatal("expected error")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("handshake ignored cancellation")
	}
}
func TestStreamDialDoesNotBlockOtherTargetOrClose(t *testing.T) {
	url, accepted := stalledPeer(t, false, false)
	tr := NewStreamTransport("", 1, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dialDone := make(chan struct{})
	go func() { tr.stream(ctx, url, cell.Scope{}); close(dialDone) }()
	<-accepted
	other := make(chan error, 1)
	go func() { _, e := tr.stream(ctx, "http://127.0.0.1:1", cell.Scope{}); other <- e }()
	select {
	case <-other:
	case <-time.After(500 * time.Millisecond):
		t.Error("unrelated target blocked by handshake")
	}
	closed := make(chan struct{})
	go func() { tr.Close(); close(closed) }()
	select {
	case <-closed:
		select {
		case <-dialDone:
		case <-time.After(time.Second):
			t.Error("Close did not cancel handshake")
		}
		if _, err := tr.stream(context.Background(), url, cell.Scope{}); err == nil {
			t.Error("closed transport accepted new stream")
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("Close blocked by handshake")
	}
}

func TestStreamWriteCancellation(t *testing.T) {
	url, _ := stalledPeer(t, true, false)
	s, e := dialRawStream(context.Background(), url, "", "", 1)
	if e != nil {
		t.Fatal(e)
	}
	defer s.fail(io.EOF)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, e := s.submit(ctx, make([]byte, 32<<20)); done <- e }()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("write ignored cancellation")
	}
}
func TestStreamAsyncCancellation(t *testing.T) {
	url, _ := stalledPeer(t, true, true)
	s, e := dialRawStream(context.Background(), url, "", "", 1)
	if e != nil {
		t.Fatal(e)
	}
	defer s.fail(io.EOF)
	ctx, cancel := context.WithCancel(context.Background())
	ch := s.sendAsync(ctx, []byte("frame"))
	cancel()
	select {
	case e := <-ch:
		if e == nil {
			t.Fatal("cancelled request reported success")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("async ack ignored cancellation")
	}
}

func TestStreamAckStallRetiresLane(t *testing.T) {
	url, _ := stalledPeer(t, true, true)
	s, err := dialRawStream(context.Background(), url, "", "", 2)
	if err != nil {
		t.Fatal(err)
	}
	defer s.fail(io.EOF)
	s.ackTimeout = 50 * time.Millisecond
	first := s.sendAsync(context.Background(), []byte("a"))
	second := s.sendAsync(context.Background(), []byte("b"))
	for _, ch := range []<-chan error{first, second} {
		select {
		case err := <-ch:
			if err == nil {
				t.Fatal("unacknowledged write succeeded")
			}
		case <-time.After(time.Second):
			t.Fatal("stalled ACK not retired")
		}
	}
	if !s.isFailed() || len(s.slots) != 0 {
		t.Fatalf("failed=%v slots=%d", s.isFailed(), len(s.slots))
	}
}

func TestStreamCancelDoesNotAbortOtherAcknowledgements(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	allowAck := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, e := l.Accept()
		if e != nil {
			return
		}
		defer c.Close()
		r := bufio.NewReader(c)
		http.ReadRequest(r)
		fmt.Fprintf(c, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: %s\r\n\r\n", StreamProtocol)
		var buf [2]byte
		io.ReadFull(r, buf[:])
		<-allowAck
		c.Write([]byte{1, 1})
		io.Copy(io.Discard, r)
	}()
	s, err := dialRawStream(context.Background(), "http://"+l.Addr().String(), "", "", 2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.fail(io.EOF); <-done }()
	s.ackTimeout = 200 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	first := s.sendAsync(ctx, []byte{1})
	second := s.sendAsync(context.Background(), []byte{2})
	cancel()
	if err := <-first; err == nil {
		t.Fatal("expected cancellation")
	}
	close(allowAck)
	select {
	case err := <-second:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("other call lost ACK")
	}
	// No outstanding frame: an idle connection must survive the stall budget.
	time.Sleep(250 * time.Millisecond)
	if s.isFailed() || len(s.slots) != 0 {
		t.Fatalf("failed=%v slots=%d", s.isFailed(), len(s.slots))
	}
}

func TestStreamRetiredLaneReconnects(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 2; i++ {
			c, e := l.Accept()
			if e != nil {
				return
			}
			r := bufio.NewReader(c)
			http.ReadRequest(r)
			fmt.Fprintf(c, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: %s\r\n\r\n", StreamProtocol)
			var b [1]byte
			io.ReadFull(r, b[:])
			if i == 1 {
				c.Write([]byte{1})
			}
			io.Copy(io.Discard, r)
			c.Close()
		}
	}()
	tr := NewStreamTransport("", 1, 1)
	defer func() { tr.Close(); l.Close(); <-done }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	first, err := tr.stream(ctx, "http://"+l.Addr().String(), cell.Scope{})
	if err != nil {
		t.Fatal(err)
	}
	first.ackTimeout = 50 * time.Millisecond
	if err := first.send(ctx, []byte{1}); err == nil {
		t.Fatal("missing ACK succeeded")
	}
	second, err := tr.stream(ctx, "http://"+l.Addr().String(), cell.Scope{})
	if err != nil {
		t.Fatal(err)
	}
	if second == first {
		t.Fatal("retired stream reused")
	}
	if err := second.send(ctx, []byte{1}); err != nil {
		t.Fatal(err)
	}
}

func TestStreamConcurrentAckOrder(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, e := l.Accept()
		if e != nil {
			return
		}
		defer c.Close()
		r := bufio.NewReader(c)
		http.ReadRequest(r)
		fmt.Fprintf(c, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: %s\r\n\r\n", StreamProtocol)
		for {
			v, e := r.ReadByte()
			if e != nil {
				return
			}
			if _, e = c.Write([]byte{v}); e != nil {
				return
			}
		}
	}()
	s, err := dialRawStream(context.Background(), "http://"+l.Addr().String(), "", "", 8)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.fail(io.EOF); <-done }()
	result := make(chan error, 100)
	for i := 0; i < 100; i++ {
		go func(v byte) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			e := s.send(ctx, []byte{v})
			if (e == nil) != (v == 1) {
				result <- fmt.Errorf("ACK mismatch byte=%d error=%v", v, e)
			} else {
				result <- nil
			}
		}(byte(i % 2))
	}
	for i := 0; i < 100; i++ {
		if e := <-result; e != nil {
			t.Error(e)
		}
	}
}

func TestStreamCoalescedDialFailureDoesNotSerializeRetries(t *testing.T) {
	url, accepted := stalledPeer(t, false, false)
	tr := NewStreamTransport("", 1, 1)
	defer tr.Close()
	leader, cancelLeader := context.WithCancel(context.Background())
	defer cancelLeader()
	go func() { tr.stream(leader, url, cell.Scope{}) }()
	<-accepted
	waitCtx, cancelWait := context.WithTimeout(context.Background(), time.Second)
	defer cancelWait()
	done := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() { _, err := tr.stream(waitCtx, url, cell.Scope{}); done <- err }()
	}
	// Allow followers to join the one in-flight dial before failing it.
	time.Sleep(30 * time.Millisecond)
	cancelLeader()
	timer := time.NewTimer(300 * time.Millisecond)
	defer timer.Stop()
	for i := 0; i < 8; i++ {
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("stalled dial succeeded")
			}
		case <-timer.C:
			t.Fatal("dial waiters serialized fresh failed handshakes")
		}
	}
}
