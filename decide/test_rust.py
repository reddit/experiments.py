import rust

decider = rust.init("decision_maker_name", "cfg.json")
if decider:
    rust.PyDecider.printer(decider)
else:
    print("Failed to init decider")