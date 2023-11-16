BS = '\\'
must_escape = (BS, '\'', '`')


def quote_identifier(identifier: str):
    first_char = identifier[0]
    if first_char in ('`', '"') and identifier[-1] == first_char:
        # Identifier is already quoted, assume that it's valid
        return identifier
    return f'`{escape_str(identifier)}`'


def escape_str(value: str):
    return ''.join(f'{BS}{c}' if c in must_escape else c for c in value)
