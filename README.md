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
