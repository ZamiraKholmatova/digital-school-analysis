import random

login_length = 6
password_length = 8
output_file = "logins_passwords.csv"


def get_random_string(character_pool, length):
    selected = random.choices(character_pool, k=length)
    return "".join(selected)


def generate_login(existing_logins, length):
    login_characters = "0123456789"
    login_candidate = get_random_string(login_characters, length)
    while login_candidate in existing_logins:
        login_candidate = get_random_string(login_characters, length)
    return login_candidate


def generate_password(existing_passwords, length):
    # character from each group should appear in password
    password_character_groups = ["0123456789", "qwertyuiopasdfghjklzxcvbnm", "QWERTYUIOPASDFGHJKLZXCVBNM"]

    assert length > len(password_character_groups), "Password is too short for specified character groups"

    def get_password_from_groups():
        num_groups = len(password_character_groups)
        new_password = ""

        current_group = 0
        groups_left = len(password_character_groups) - (current_group + 1)
        current_group_max_len = length - groups_left - len(new_password)
        
        for ind, group in enumerate(password_character_groups):
            if ind == len(password_character_groups) - 1:
                current_group_len = password_length - len(new_password)
            else:
                current_group_len = random.randint(1, current_group_max_len)
            new_password += get_random_string(group, current_group_len)
            
            current_group += 1
            groups_left = len(password_character_groups) - (current_group + 1)
            current_group_max_len = length - groups_left - len(new_password)
        
        password = [c for c in new_password]
        random.shuffle(password)
        password = "".join(password)
        assert len(password) == password_length, "Password length is {} but should be {}".format(len(password), password_length)

        return password
    
    password_candidate = get_password_from_groups()
    while password_candidate in existing_passwords:
        password_candidate = get_password_from_groups()
    return password_candidate


used_string = set()

needed_pairs = 48000

logins = set()
passwords = set()

with open(output_file, "w") as login_password_table:
    login_password_table.write("login,password\n")
    for _ in range(needed_pairs):
        login = generate_login(used_string, login_length)
        password = generate_password(used_string, password_length)

        # make sure that 
        used_string.add(login)
        used_string.add(password)
        
        login_password_table.write("{},{}\n".format(login, password))

        logins.add(login)
        passwords.add(password)

print("Generated {} unique logins and {} unique passwords and saved into {}".format(len(logins), len(passwords), output_file))
