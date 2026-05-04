# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 1.10.11

### Fixed

- REDCap PID 247 `adult_consent.parent_involvement` → PID 625.

## 1.10.10

### Added

- EEG eligibility responses from REDCap consent project to REDCap operations project.

## 1.10.9

### Fixed

- Harmonized conflicting datetime formats.
- Removed reduplicitave field names in REDCap invitation instruments.

## 1.10.8

### Changed

- Set `permission_audiovideo_participant` to "Not Applicable" when participant's age is < 11 or ≥ 18 years.

## 1.10.7

### Fixed

- Cache delay of 1 cycle for new subjects from Ripple.

## 1.10.6

### Added

- Protection against log injection attacks.

### Fixed

- Column misalignment in Ripple-to-REDCap.

## 1.10.5

### Fixed

- Convert float values to int for `redcap_repeat_instance`.

## 1.10.4

### Changed

- Updating caching logic to incorporate full state, not just record ID.

### Fixed

- Convert "parent_involvement" from a set to a list before JSON serializing.

## 1.10.3

### Fixed

- Checking for fields we know don't exist in REDCap.

## 1.10.2

### Changed

- Restored minute-by-minute jobs pending AWS permission update.

## 1.10.1

### Added

- Endpoints to recieve REDCap Data Entry Triggers.

### Changed

- Updated `curious_account_created` tracking.

### Fixed

- Websocket now gets a new token when the one it's trying expires.
- Bug in creating new Curious users after splitting REDCap and Curious projects.

## 1.10.0

### Added

- REDCap &laquo;HBN - Responder Tracking (PID 879)&raquo; authentication.
- Fields "r_id", "curious_email_child" and "curious_password_child" for PID 625 to Curious.

### Changed

- REDCap-to-Curious data now comes from PID 625.
- Temporarily disabled connection to PID 879 in favor of manual `r_id` field in PID 625.
- Handle more datetime options in `mindlogger-autoexport`.

### Fixed

- Send timestamps to Curious API in UTC.

### Deprecated

- `hbnmigration.from_redcap.config.Fields.export_247`

## 1.9.4

### Fixed

- Split config for Curious invitations.

## 1.9.3

### Added

- Exception handling for REDCap timeout.

## 1.9.2

### Fixed

- Commented out missing fields.

## 1.9.1

### Fixed

- Curious invitations CLI.
- Config keys to set Curious applet credential environment variable names.

## 1.9.0

### Changed

- Split parent-report and self-report into separate Curious applets.
- Updated mappings for REDCap PID 247 to PID 625.
- Moved `enrollment_complete` from PID 247 to PID 625.

### Upgraded

- Python@3.14

### Deprecated

- `._config_variables.curious_variables.curious_variables.activity_ids`
- `._config_variables.curious_variables.curious_variables.applet_ids`
- `._config_variables.curious_variables.curious_variables.Credentials`
- `._config_variables.curious_variables.curious_variables.AppletCredentials.hbn_mindlogger`

## 1.8.0

### Added

- Alerts to Microsoft Teams on certain failures.

### Changed

- Look up indices for options in REDCap instead of following `{Curious index} + 1` heuristic.
- Check for duplicate data and skip those records when copying data from Curious to REDCap.

### Fixed

- Bug where MRNs & REDCap record IDs didn't match in data from Curious to REDCap (using MRN as record ID).

## 1.7.1

### Added

- Timeouts to minute-by-minute transfers to avoid hanging on connection failures.

### Changed

- Curious data and alerts now go to prod REDCap project even in dev mode.

## 1.7.0

### Changed

- Moved project status to envrironment variable `$HBNMIGRATION_PROJECT_STATUS` with default `"prod"`
- Log missing fields but push acceptable data in those cases
- Log invalid categories separately
- Optimize from day-by-day to minute-by-minute

### Fixed

- Don't create `{mrn}_P` records in REDCap.
- Handle differences in websocket and HTTPS Curious alerts API responses.

## 1.6.1

### Added

- `no alerts triggered` set for potential alerts that did completed without triggering

## 1.6.0

### Changed

- REDCap status changed from dev to prod

## 1.5.0

### Added

- `mrn_error_log.tsv`
- Python scripts / jobs
  - Curious data to REDCap
    - `{activity_name}_start_date` to Curious instruments in REDCap

### Fixed

- bug in pushing Curous alerts to REDCap
- bug in pushing REDCap participants to Curious

## 1.4.1

### Added

- Python scripts / jobs
  - Curious invitations to REDCap
- TypeScript scripts / jobs
  - decrypt single answer

## 1.4.0

### Updated

- REDCap PID 744 `record_id`s now match `mrn`s
- When setting `"complete_parent_second_guardian_consent"` in PID 744:
   <table>
     <tbody>
       <tr><th colspan="2">if</th><th>then</th></tr>
       <tr><th colspan="2">PID 247</th><th>PID 744</th></tr>
       <tr><th><code>["guardian2_consent"]</code></th><th><code>["parent_second_guardian_consent_complete"]</code></th><th><code>["complete_parent_second_guardian_consent"]</code></th></tr>
       <tr><td><code>"No"</code></td><td rowspan="2">any</td><td><code>"Not Required"</code></td></tr>
       <tr><td><code>"Not Applicable (Adult Participant)"</code></td><td><code>"Not Applicable (Adult Participant)"</code></td></tr>
       <tr><td rowspan="3">not in <code>["No", "Not Applicable (Adult Participant)"]</code></td><td><code>"Incomplete"</code></td><td><code>"Incomplete"</code></td></tr>
       <tr><td><code>"Unverified"</code></td><td><code>"Unverified"</code></td></tr>
       <tr><td><code>"Complete"</code></td><td><code>"Complete"</code></td></tr>
      </tbody>
    </table>

## 1.3.0

### Added

- Python scripts / jobs
  - Curious alerts to REDCap

### Updated

- initial Terraform configuration
- utility function library

## 1.2.0

### Added

- Python scripts / jobs
  - REDCap to Curious

### Updated

- initial Terraform configuration
- utility function library

## 1.1.0

### Added

- Python scripts / jobs
  - REDCap to REDCap

### Updated

- initial Terraform configuration
- utility function library

## 1.0.0

### Added

- initial Terraform configuration
- utility function library
- Python scripts / jobs
  - Ripple to REDCap
