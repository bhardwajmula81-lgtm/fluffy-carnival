"""In-memory query matching; no filesystem work or Qt item creation."""
import fnmatch
import re

_FIELD = re.compile(r'^(rtl|block|user|owner|source|stage|log|status|path|run|name|type|runtime|start|end|note):(.*)$', re.I)


def stage_key(stage):
    return (str(stage.get('name', '')), str(stage.get('_origin_be_path', '')),
            str(stage.get('_origin_stage_path') or stage.get('stage_path', '')))


def parse_query(query):
    terms = []
    for part in str(query or '').strip().lower().split(';'):
        part = part.strip()
        if part:
            match = _FIELD.match(part)
            terms.append((match.group(1).lower(), match.group(2).strip())
                         if match else (None, part))
    return terms


def _matches(value, needle, exact=False):
    value = str(value or '').lower()
    if not needle:
        return False
    if '*' in needle or '?' in needle or '[' in needle:
        return fnmatch.fnmatchcase(value, needle if exact else '*' + needle + '*')
    return value == needle if exact else needle in value


def match_run(run, terms, notes=''):
    """All terms must match the run itself or the SAME stage within it."""
    info = run.get('info') or {}
    fields = {k: run.get(k, '') for k in ('rtl', 'block', 'source', 'path')}
    fields.update(run=run.get('r_name', ''), name=run.get('r_name', ''),
                  type=run.get('run_type', ''), note=notes,
                  status=' '.join(str(run.get(k, '')) for k in
                                  ('fe_status', 'st_n', 'st_u', 'vslp_status')),
                  user='{} {}'.format(run.get('owner', ''), run.get('user', '')),
                  log=run.get('log_path', ''))
    fields['owner'] = fields['user']
    fields.update({k: info.get(k, '') for k in ('runtime', 'start', 'end')})
    blob = ' '.join(str(v) for v in fields.values()) if any(key is None for key, _ in terms) else ''
    direct = []
    for key, value in terms:
        direct.append(key != 'stage' and _matches(fields.get(key, '') if key else blob, value))
    if not terms:
        return False, set()
    direct_hit = all(direct)
    if direct_hit:
        return True, set()
    stage_fields_supported = {None, 'stage', 'status', 'path', 'log', 'source', 'runtime', 'start', 'end'}
    if any(not ok and key not in stage_fields_supported for (key, _), ok in zip(terms, direct)):
        return False, set()
    stage_terms = [(key, value) for (key, value), ok in zip(terms, direct) if not ok or key == 'source']
    hits = set()
    for stage in run.get('stages') or []:
        matched = True
        for key, value in stage_terms:
            if key == 'stage': text = stage.get('name', '')
            elif key == 'status': text = stage.get('stage_status', '')
            elif key == 'path': text = stage.get('_origin_stage_path') or stage.get('stage_path', '')
            elif key == 'log': text = stage.get('log') or stage.get('log_path', '')
            elif key == 'source': text = stage.get('_origin_source') or stage.get('source') or fields['source']
            elif key in ('runtime', 'start', 'end'): text = (stage.get('info') or {}).get(key, '')
            else: text = ' '.join(str(stage.get(k, '')) for k in ('name', 'stage_status', 'stage_path', 'log'))
            if not _matches(text, value, exact=key == 'stage'):
                matched = False
                break
        if matched:
            hits.add(stage_key(stage))
    # A run-name match should navigate to the run, not every stage below it.
    return direct_hit, set() if direct_hit else hits
