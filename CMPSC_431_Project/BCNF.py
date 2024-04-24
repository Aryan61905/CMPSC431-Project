def convert_to_bcnf(schema):
    # Helper function to check if a set of attributes is a superkey
    def is_superkey(attributes, functional_dependencies):
        for fd in functional_dependencies:
            if set(fd[0]).issubset(attributes) and not set(fd[1]).issubset(attributes):
                return False
        return True

    # Helper function to find the closure of a set of attributes
    def closure(attributes, functional_dependencies):
        closure_set = set(attributes)
        changed = True
        while changed:
            changed = False
            for fd in functional_dependencies:
                if set(fd[0]).issubset(closure_set) and not set(fd[1]).issubset(closure_set):
                    closure_set |= set(fd[1])
                    changed = True
        return closure_set

    # Helper function to decompose a relation into BCNF
    def decompose_relation(relation, functional_dependencies):
        decomposed_relations = []
        for fd in functional_dependencies:
            left_closure = closure(fd[0], functional_dependencies)
            if not is_superkey(left_closure, functional_dependencies):
                # If the left side of the FD is not a superkey, decompose
                relation1 = left_closure | set(fd[1])
                relation2 = relation - left_closure | set(fd[0])
                decomposed_relations.append(relation1)
                decomposed_relations.append(relation2)
                return decomposed_relations
        # If all functional dependencies satisfied, return the original relation
        return [relation]

    # Parse the schema into relation names and their attributes
    relations = {}
    for entry in schema:
        relation_name, attribute = entry[0], entry[1]
        if relation_name in relations:
            relations[relation_name].append(attribute)
        else:
            relations[relation_name] = [attribute]

    # Extract functional dependencies
    functional_dependencies = [(set(fd[0].split(',')), set(fd[1].split(','))) for fd in schema]

    # Decompose each relation into BCNF
    bcnf_relations = {}
    for relation_name, attributes in relations.items():
        decomposed_relations = decompose_relation(set(attributes), functional_dependencies)
        bcnf_relations[relation_name] = decomposed_relations

    return bcnf_relations

# Example schema
schema = [
    ("Stadiums", "arena,text"),
    ("Stadiums", "team,text"),
    ("Stadiums", "stadium_id,integer"),
    ("Injury", "player,text"),
    ("Injury", "team,text"),
    ("Injury", "update,text"),
    ("Injury", "description,text"),
    ("Injury", "injury_id,integer"),
    ("Teams", "team,text"),
    ("Teams", "g,integer"),
    ("Teams", "team_id,integer"),
    ("Players", "player,text"),
    ("Players", "pos,text"),
    ("Players", "age,integer"),
    ("Players", "team,text"),
    ("Players", "g,integer"),
    ("Players", "mp,real"),
    ("Players", "player_id,integer"),
    ("Coaches", "coach,text"),
    ("Coaches", "team,text"),
    ("Coaches", "g,integer"),
    ("Coaches", "w,integer"),
    ("Coaches", "l,integer"),
    ("Coaches", "coach_id,integer"),
    ("Player_Offensive_stats", "player,text"),
    ("Player_Offensive_stats", "pts,real"),
    ("Player_Offensive_stats", "trb,real"),
    ("Player_Offensive_stats", "ast,real"),
    ("Player_Offensive_stats", "player_id,integer"),
    ("Western_conference", "team,text"),
    ("Western_conference", "w,integer"),
    ("Western_conference", "l,integer"),
    ("Western_conference", "western_conference_id,integer"),
    ("Team_Stats", "team,text"),
    ("Team_Stats", "mp,real"),
    ("Team_Stats", "pts,real"),
    ("Team_Stats", "trb,real"),
    ("Team_Stats", "ast,real"),
    ("Team_Stats", "stl,real"),
    ("Team_Stats", "blk,real"),
    ("Team_Stats", "team_id,integer"),
    ("Player_Defensive_stats", "player,text"),
    ("Player_Defensive_stats", "stl,real"),
    ("Player_Defensive_stats", "blk,real"),
    ("Player_Defensive_stats", "player_id,integer"),
    ("Eastern_conference", "team,text"),
    ("Eastern_conference", "w,integer"),
    ("Eastern_conference", "l,integer"),
    ("Eastern_conference", "eastern_conference_id,integer")
]

# Convert to BCNF
bcnf_relations = convert_to_bcnf(schema)
for relation_name, attributes in bcnf_relations.items():
    print(relation_name + ":")
    for attr in attributes:
        print(attr)
    print()