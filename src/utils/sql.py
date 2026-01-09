def load_named_queries(sql_path: str) -> dict:
    queries = {}
    current_name = None
    buffer = []
    with open(sql_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip().startswith("-- name:"):
                if current_name:
                    queries[current_name] = "".join(buffer).strip()
                    buffer = []
                current_name = line.split(":", 1)[1].strip()
            else:
                buffer.append(line)
        if current_name:
            queries[current_name] = "".join(buffer).strip()
    return queries