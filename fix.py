with open("bench/mini_kv_bench.cpp", "r") as f:
    content = f.read()

content = content.replace("struct Scenario\n    CompactionType ctype;\n{", "struct Scenario\n{\n    CompactionType ctype;")
content = content.replace("struct ScenarioResult\n    CompactionType ctype;\n{", "struct ScenarioResult\n{")

with open("bench/mini_kv_bench.cpp", "w") as f:
    f.write(content)
