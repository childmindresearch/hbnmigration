# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 1.7.0

### Changed

- Moved project status to envrironment variable `$HBNMIGRATION_PROJECT_STATUS` with default `"prod"`
- Log missing fields but push acceptable data in those cases
- Log invalid categories separately
- Optimize from day-by-day to minute-by-minute

### Fixed

- Don't create `{mrn}_P` records in REDCap.

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
