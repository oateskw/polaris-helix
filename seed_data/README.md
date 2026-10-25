# seed_data

The configuration rows Helix cannot recreate by itself, kept as JSON so a lost database is a one-command rebuild.

```bash
python init_db.py --dry-run      # validate these files, contact nothing
python init_db.py                # create the schema, then load these files (keeps existing rows)
python init_db.py --overwrite    # replace existing rows with these values
python init_db.py --no-seed      # schema only
python export_seed.py            # write the live database back to these files
```

## Files

| File | Table | Key | Loaded? |
| --- | --- | --- | --- |
| `helix_agent_prompts.json` | `helix_agent_prompts` | `(agent_name, prompt_key)` | yes |
| `helix_agent_manifests.json` | `helix_agent_manifests` | `agent_name` (lowercase) | yes |
| `helix_brain_scheduler.json` | `helix_brain_scheduler` | `industry_name` | **you create it** from the `.example` |
| `helix_agent_schedule.json` | `helix_agent_schedule` | `(agent_name, weekday_int)` | **you create it** from the `.example` |

`*.example.json` files are templates and are never loaded.

## Rules

- **Existing rows are kept by default.** Helix's optimizer rewrites prompts and you change the schedule from Telegram, so a plain `init_db.py` never undoes that. Use `--overwrite` only when you mean it.
- **Files are validated before the database is touched**, and the load is one transaction: a typo in any file changes nothing.
- **After you change a prompt, industry, manifest or the schedule, run `python export_seed.py` and commit the result.** That is your backup of the things that cannot be re-harvested.
- **Never put lead data, API keys, or tokens in these files.** `cold_leads`, logs and memory are intentionally not exported. If the repository is shared, remember that prompts and industry lists are your business strategy.

## What is in the starting files

- **`helix_agent_prompts.json`** has only the two Chiron prompts, `reactive_healer` and `proactive_optimizer`. They were written fresh to replace lost originals and follow the exact `SEARCH`/`REPLACE` reply format Chiron's parser requires. Chiron does nothing without them. Prompts for the other agents were not recoverable: add them to this file if you have them (the optimizer only edits rows that exist).
- **`helix_agent_manifests.json`** describes all ten agents, with the correct repository for each and the files Chiron should look at. `is_active` is `true` only for the six agents Helix audits (Nexus, Beacon, Radar, Pulsar, Nova, Vanguard). Helix, Chiron, Vector and Aura are `false` so Helix's automatic audit does not send Chiron to rewrite the orchestrator or Chiron itself; flip them on deliberately.
- **Industries and the schedule are yours.** Copy each `.example.json`, fill in your real values, and load. Until `helix_agent_schedule` has enabled rows (or you send `/enable <agent>` in Telegram), the awareness loop has nothing to dispatch. In `helix_agent_schedule`, `weekday_int` counts from 0 = Sunday.
