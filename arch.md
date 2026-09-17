                         COMMON PROVIDER CODE

Slack HTTP request
    ↓
Slack ingress
    ├── verify
    ├── parse
    ├── classify
    ├── select Slack callback
    └── derive mailbox key
          ↓
Delivery admission


                 SERVER                         CLOUDFLARE

        Postgres mailbox write             Call mailbox DO
                  ↓                               ↓
            return HTTP 2xx                DO storage transaction
                                                  ├── save event
                                                  └── set alarm
                                                       ↓
                                                 return HTTP 2xx

        Long-running poller                Cloudflare alarm
                  ↓                               ↓
        scan ready mailbox keys            mailbox key already known
                  ↓                               ↓
            processMailbox                 processMailbox
                  └──────────────┬────────────────┘
                                 ↓
                       Shared delivery engine
                         ├── apply ordering
                         ├── claim work
                         ├── invoke handler
                         ├── record completion
                         └── schedule retry state
                                 ↓
                       Provider handler wrapper
                                 ↓
                       Application callback
                                 ↓
                         Slack service
                                 ↓
                           Slack API