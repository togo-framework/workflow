<!-- togo-header -->
<div align="center">
  <picture><source media="(prefers-color-scheme: dark)" srcset=".github/assets/togo-mark-dark.svg" /><img src=".github/assets/togo-mark.svg" alt="ToGO" height="64" /></picture>
  <h1>togo-framework/workflow</h1>
  <p>
    <a href="https://to-go.dev/marketplace"><img src="https://img.shields.io/badge/marketplace-to--go.dev-1F8A99" alt="marketplace" /></a>
    <a href="https://pkg.go.dev/github.com/togo-framework/workflow"><img src="https://pkg.go.dev/badge/github.com/togo-framework/workflow.svg" alt="pkg.go.dev" /></a>
    <img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT" />
  </p>
  <p><strong>Part of the <a href="https://to-go.dev">togo</a> framework.</strong></p>
</div>

## Install

```bash
togo install togo-framework/workflow
```

<!-- /togo-header -->

# workflow — togo workflow engine

Run **dynamic, multi-step workflows** over the togo [worker/queue](https://to-go.dev).
A workflow is an ordered list of steps; each step has a **type** that maps to a
registered handler, so any plugin can be used as a workflow step (send a mail,
charge a payment, call an AI model, sync contacts…). Steps run **asynchronously
through the kernel Queue**, so long pipelines never block a request.

```bash
togo install togo-framework/workflow
togo install togo-framework/queue     # the runtime (memory/redis/db)
```

## Register step types (from any plugin)

```go
import "github.com/togo-framework/workflow"

func init() {
    workflow.RegisterStep("mail.send", func(ctx context.Context, sc *workflow.StepContext, with map[string]any) error {
        m, _ := mail.FromKernel(sc.Kernel)
        return m.Send(ctx, mail.Message{To: []string{with["to"].(string)}, Subject: with["subject"].(string)})
    })
}
```

## Define + trigger a workflow

```go
wf, _ := workflow.FromKernel(k)
wf.Define(workflow.Workflow{Name: "welcome", Steps: []workflow.Step{
    {Type: "mail.send", With: map[string]any{"to": "{{user.email}}", "subject": "Welcome!"}},
    {Type: "contacts.sync"},
}})

run, _ := wf.Trigger(ctx, "welcome", map[string]any{"user": user})
// run.Status: pending → running → done | failed; steps execute over the queue.
```

A step can set `sc.Next` to jump/branch (or `-1` to end early) and read/write
`sc.Run.State` shared across the run. With no queue installed, steps run inline.

MIT

<!-- togo-sponsors -->
---

<div align="center">
  <h3>Premium sponsors</h3>
  <p>
    <a href="https://id8media.com"><strong>ID8 Media</strong></a> &nbsp;·&nbsp;
    <a href="https://one-studio.co"><strong>One Studio</strong></a>
  </p>
  <p><sub>Support togo — <a href="https://github.com/sponsors/fadymondy">become a sponsor</a>.</sub></p>
</div>
<!-- /togo-sponsors -->
