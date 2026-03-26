# from REDCap

## to REDCap

```mermaid
---
config:
  look: handDrawn
  themeVariables:
    edgeLabelBackground: '#FAF9F500'
    fontFamily: "Verdana, Courier New, Arial Black, Arial Bold, cursive, fantasy"
---
flowchart TD
    classDef apiCall color:#F56200,fill:#0067a025,stroke:#0067a0,stroke-width:4px;
    classDef default stroke:#0067a0,color:#0067a0,fill:#FAF9F510;

    start@{ shape: circle, label: "redcap-to-redcap" } --> fetch_247@{ shape: rounded, label: "POST «REDCap»\n*PID 247*" } --> has_data@{ shape: diamond, label: "any subjects with <code style='color:#F56200'>intake_ready</code> == \"Ready to Send to Intake Redcap\"?" } -- no --> stop@{ shape: dbl-circ, label: "Stop" };
    has_data -- yes --> transform@{ shape: rounded, label: "transform" } --> push_744@{ shape: rounded, label: "POST «REDCap»\n*PID 744*" } --> update_247@{ shape: rounded, label: "POST «REDCap»\n*PID 247*\n(set <code style='color:#F56200'>intake_ready</code> = \"Participant information already sent to HBN - Intake Redcap project\")" } --> stop;

    class fetch_247,push_744,update_247 apiCall;
    linkStyle 0,1,2,3,4,5,6 stroke:#0067a0,color:#0067a0
````

## to Curious

```mermaid
---
config:
  look: handDrawn
  themeVariables:
    edgeLabelBackground: '#FAF9F500'
    fontFamily: "Verdana, Courier New, Arial Black, Arial Bold, cursive, fantasy"
---
flowchart TD
    classDef apiCall color:#F56200,fill:#0067a025,stroke:#0067a0,stroke-width:4px;
    classDef default stroke:#0067a0,color:#0067a0,fill:#FAF9F510;

    start@{ shape: circle, label: "redcap-to-curious" } --> fetch_247@{ shape: rounded, label: "POST «REDCap»\n*PID 247*" } --> has_data@{ shape: diamond, label: "any subjects with <code style='color:#0067a0'>enrollment_complete</code> == \"Ready to Send to Curious\"?" } -- no --> stop@{ shape: dbl-circ, label: "Stop" };
    has_data -- yes --> transform@{ shape: rounded, label: "transform" } --> new_curious_account@{ shape: procs, label: "(POST «Curious»/invitations/{applet_id}/{account_type}) × (number of particpants + parents)" } --> update_247@{ shape: rounded, label: "POST «REDCap»\n*PID 247*\n(set <code style='color:#F56200'>enrollment_complete</code> = \"Parent and Participant information already sent to Curious\")" } --> stop;

    class fetch_247,new_curious_account,update_247 apiCall;
    linkStyle 0,1,2,3,4,5,6 stroke:#0067a0,color:#0067a0
```
