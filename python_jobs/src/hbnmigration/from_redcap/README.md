# from REDCap

## to REDCap

```mermaid
flowchart TD
    classDef apiCall fill:#0067a0,stroke:#0067a0,stroke-width:4px;
    start@{ shape: circle, label: "redcap-to-redcap" } --> fetch_247@{ shape: rounded, label: "POST «REDCap»\n*PID 247*" } --> has_data@{ shape: diamond, label: "any subjects with <code>intake_ready</code> == \"Ready to Send to Intake Redcap\"?" } -- no --> stop@{ shape: dbl-circ, label: "Stop" };
    has_data -- yes --> transform@{ shape: rounded, label: "transform" } --> push_744@{ shape: rounded, label: "POST «REDCap»\n*PID 744*" } --> update_247@{ shape: rounded, label: "POST «REDCap»\n*PID 247*\n(set <code>intake_ready</code> == \"Participant information already sent to HBN - Intake Redcap project\")" } --> stop;
    class fetch_247,push_744,update_247 apiCall;
````
