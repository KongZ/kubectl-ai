// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package slack

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/kubectl-ai/pkg/agent"
	"github.com/GoogleCloudPlatform/kubectl-ai/pkg/api"
	"github.com/GoogleCloudPlatform/kubectl-ai/pkg/sessions"
	"github.com/GoogleCloudPlatform/kubectl-ai/pkg/ui"
	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"k8s.io/klog/v2"
)

// SlackAPI defines the interface for Slack client methods used by SlackUI
type SlackAPI interface {
	PostMessage(channelID string, options ...slack.MsgOption) (string, string, error)
	UploadFileV2(params slack.UploadFileV2Parameters) (*slack.FileSummary, error)
}

// AgentManager defines the interface for agent management methods used by SlackUI
type AgentManager interface {
	GetAgent(ctx context.Context, sessionID string) (*agent.Agent, error)
	SetAgentCreatedCallback(func(*agent.Agent))
}

type SlackUI struct {
	httpServer         *http.Server
	httpServerListener net.Listener

	manager         AgentManager
	sessionManager  *sessions.SessionManager
	defaultModel    string
	defaultProvider string
	botToken        string
	signingSecret   string
	apiClient       SlackAPI

	mu sync.Mutex
}

var _ ui.UI = &SlackUI{}

func NewSlackUI(manager AgentManager, sessionManager *sessions.SessionManager, defaultModel, defaultProvider, listenAddress string) (*SlackUI, error) {
	botToken := os.Getenv("SLACK_BOT_TOKEN")
	signingSecret := os.Getenv("SLACK_SIGNING_SECRET")
	if botToken == "" || signingSecret == "" {
		return nil, fmt.Errorf("SLACK_BOT_TOKEN and SLACK_SIGNING_SECRET environment variable not set")
	}

	apiClient := slack.New(botToken)

	s := &SlackUI{
		manager:         manager,
		sessionManager:  sessionManager,
		defaultModel:    defaultModel,
		defaultProvider: defaultProvider,
		botToken:        botToken,
		signingSecret:   signingSecret,
		apiClient:       apiClient,
	}

	// Register callback to listen to new agents
	manager.SetAgentCreatedCallback(func(a *agent.Agent) {
		s.ensureAgentListener(a)
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/slack/events", s.handleSlackEvents)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	httpServer := &http.Server{
		Addr:    listenAddress,
		Handler: mux,
	}

	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return nil, fmt.Errorf("starting slack ui listener: %w", err)
	}

	endpoint := listener.Addr()
	s.httpServerListener = listener
	s.httpServer = httpServer

	fmt.Fprintf(os.Stdout, "listening on http://%s\n", endpoint)
	return s, nil
}

func (s *SlackUI) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		if err := s.httpServer.Serve(s.httpServerListener); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.httpServer.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

func (s *SlackUI) ClearScreen() {
	// Not applicable
}

func (s *SlackUI) handleSlackEvents(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	sv, err := slack.NewSecretsVerifier(r.Header, s.signingSecret)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if _, err := sv.Write(body); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if err := sv.Ensure(); err != nil {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	eventsAPIEvent, err := slackevents.ParseEvent(json.RawMessage(body), slackevents.OptionNoVerifyToken())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if eventsAPIEvent.Type == slackevents.URLVerification {
		var r *slackevents.ChallengeResponse
		err := json.Unmarshal([]byte(body), &r)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte(r.Challenge))
		return
	}

	if eventsAPIEvent.Type == slackevents.CallbackEvent {
		innerEvent := eventsAPIEvent.InnerEvent
		switch ev := innerEvent.Data.(type) {
		case *slackevents.AppMentionEvent:
			s.processMessage(ev.Channel, ev.ThreadTimeStamp, ev.TimeStamp, ev.Text)
		case *slackevents.MessageEvent:
			// Ignore messages from bots to prevent loops
			if ev.BotID != "" {
				return
			}
			s.processMessage(ev.Channel, ev.ThreadTimeStamp, ev.TimeStamp, ev.Text)
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (s *SlackUI) processMessage(channel, threadTS, ts, text string) {
	// Clean text (remove bot mention if any)
	// Mentions look like <@U123456>
	processedText := text
	for {
		start := strings.Index(processedText, "<@")
		if start == -1 {
			break
		}
		end := strings.Index(processedText[start:], ">")
		if end == -1 {
			break
		}
		processedText = processedText[:start] + strings.TrimSpace(processedText[start+end+1:])
	}

	// If threadTS is empty, use ts (start of a new thread)
	effectiveThreadTS := threadTS
	if effectiveThreadTS == "" {
		effectiveThreadTS = ts
	}

	sessionID := fmt.Sprintf("slack-%s-%s", channel, effectiveThreadTS)
	ctx := context.Background()

	// Check if session exists
	session, err := s.sessionManager.FindSessionByID(sessionID)
	if err != nil {
		// Session not found, create new one
		meta := sessions.Metadata{
			ModelID:    s.defaultModel,
			ProviderID: s.defaultProvider,
		}
		session, err = s.sessionManager.NewSessionWithID(sessionID, meta)
		if err != nil {
			klog.Errorf("Failed to create session for Slack: %v", err)
			return
		}
		session.Name = "Slack Thread " + effectiveThreadTS
		if err := s.sessionManager.UpdateLastAccessed(session); err != nil {
			klog.Warningf("Failed to update session name: %v", err)
		}
	}

	// Get or create agent
	agent, err := s.manager.GetAgent(ctx, sessionID)
	if err != nil {
		klog.Errorf("Failed to get/create agent for Slack: %v", err)
		return
	}

	// Send message to agent
	agent.Input <- &api.UserInputResponse{Query: processedText}
}

func (s *SlackUI) ensureAgentListener(a *agent.Agent) {
	// Extract channel and thread from session ID
	sessionID := a.Session.ID
	if !strings.HasPrefix(sessionID, "slack-") {
		// This agent is not managed by Slack UI
		return
	}

	parts := strings.SplitN(sessionID, "-", 3)
	if len(parts) < 3 {
		klog.Errorf("Invalid Slack session ID format: %s", sessionID)
		return
	}
	channel := parts[1]
	threadTS := parts[2]

	// Start a single goroutine for this agent's lifetime
	go func() {
		for msg := range a.Output {
			apiMsg, ok := msg.(*api.Message)
			if !ok {
				continue
			}

			// Only post agent or model messages to Slack
			if apiMsg.Source == api.MessageSourceAgent || apiMsg.Source == api.MessageSourceModel {
				if text, ok := apiMsg.Payload.(string); ok && text != ">>>" {
					s.postToSlack(channel, threadTS, text)
				} else if choiceReq, ok := apiMsg.Payload.(*api.UserChoiceRequest); ok {
					s.postToSlack(channel, threadTS, choiceReq.Prompt)
				} else if errPayload, ok := apiMsg.Payload.(error); ok {
					s.postToSlack(channel, threadTS, "Error: "+errPayload.Error())
				}
			}
		}
		klog.Infof("Slack listener for session %s terminated", sessionID)
	}()
}

func (s *SlackUI) postToSlack(channel, threadTS, text string) {
	if isComplexOrLong(text) {
		s.uploadSnippet(channel, threadTS, text)
		return
	}

	formattedText := formatForSlack(text)

	_, _, err := s.apiClient.PostMessage(channel,
		slack.MsgOptionText(formattedText, false),
		slack.MsgOptionTS(threadTS),
	)
	if err != nil {
		klog.Errorf("Failed to post message to Slack: %v", err)
	}
}

func (s *SlackUI) uploadSnippet(channel, threadTS, text string) {
	klog.Infof("Response too long or complex, uploading as snippet to channel %s", channel)

	params := slack.UploadFileV2Parameters{
		Channel:         channel,
		ThreadTimestamp: threadTS,
		Content:         text,
		Filename:        "response.md",
		Title:           "Kubectl AI Response",
		InitialComment:  "The result is too long, here is a snippet:",
	}

	_, err := s.apiClient.UploadFileV2(params)
	if err != nil {
		klog.Errorf("Failed to upload snippet to Slack: %v", err)
		// Fallback to regular message if upload fails, but truncated
		truncated := text
		if len(truncated) > 3900 {
			truncated = truncated[:3900] + "\n... (truncated)"
		}
		formatted := formatForSlack(truncated)
		s.apiClient.PostMessage(channel, slack.MsgOptionText(formatted, false), slack.MsgOptionTS(threadTS))
	}
}

func formatForSlack(text string) string {
	// Simple Markdown to mrkdwn conversion

	// 1. Triple asterisks (Bold + Italic)
	reBoldItalic := regexp.MustCompile(`\*\*\*(.*?)\*\*\*`)
	text = reBoldItalic.ReplaceAllString(text, `*_${1}_*`)

	// 2. Double asterisks (Bold)
	// Use placeholder to avoid italic regex matching it
	reBold := regexp.MustCompile(`\*\*(.*?)\*\*`)
	text = reBold.ReplaceAllString(text, `@@@BOLD@@@${1}@@@BOLD@@@`)

	// 3. Single asterisk (Italic)
	reItalic := regexp.MustCompile(`\*([^\s\*].*?[^\s\*])\*`)
	text = reItalic.ReplaceAllString(text, `_${1}_`)

	// 4. Restore Bold
	text = strings.ReplaceAll(text, "@@@BOLD@@@", "*")

	// 5. Links: [text](url) -> <url|text>
	reLink := regexp.MustCompile(`\[(.*?)\]\((.*?)\)`)
	text = reLink.ReplaceAllString(text, `<${2}|${1}>`)

	return text
}

func isComplexOrLong(text string) bool {
	if len(text) > 3000 {
		return true
	}
	// Detect tables: usually start with | and have --- separator
	if strings.Contains(text, "|") && strings.Contains(text, "---") {
		return true
	}
	// Detect long code blocks
	if strings.Count(text, "```") >= 2 && len(text) > 1000 {
		return true
	}
	return false
}
