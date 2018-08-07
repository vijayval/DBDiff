import re 


def get_create_table_statements(file):
    count = 0
    create_table_stmts = {}
    for each in file:
        if 'CREATE TABLE' in each:
            create_table_stmts[count] = each
            inside_table = True
        elif 'DEFAULT CHARSET=utf8' in each:
            create_table_stmts[count] =   create_table_stmts[count] + each
            count = count + 1
            inside_table = False
        elif inside_table:
            ds = create_table_stmts[count]
            create_table_stmts[count] = ds + each
    return create_table_stmts

def sort_create_table_stmts(tables_dict, create_regx, fk_regex):
    tables = []
    # get all table names from create statements
    for each in range(len(tables_dict)):
        m = re.search(create_regx,tables_dict[each])
        tables.append(m.group(1))
    # reorder the table creation, if table has fk dependency 
    for each in range(len(tables_dict)):
        if 'FOREIGN KEY' in tables_dict[each]:
            pattern = re.compile(fk_regex)
            match_query = pattern.findall(tables_dict[each])
            related_tables = []
          
            for each_fk in match_query:
                related_tables.append(each_fk[2].split()[0])
        
            for each_fk in related_tables:
                if each_fk in tables[each+1:-1]:
                    tables_dict[len(tables_dict) + 1] = tables_dict[each]
                    tables_dict[each] = ''
    return tables_dict 

def segrigate_statements(file, drop_stmt_regx, alter_stmt_add_regx, alter_stmt_drop_regx, alter_stmt_change_regx, insert_stmt_regx, exceptional_tables):
    drop_statements= []
    alter_add_stmt = []
    alter_drop_stmt = []
    alter_change_stmt = []
    insert_set_stmts = []

    for each in file:
        if re.search(drop_stmt_regx,each):
            drop_statements.append(each)
        if re.search(alter_stmt_add_regx,each):
            alter_add_stmt.append(each)
        if re.search(alter_stmt_drop_regx,each):
            alter_drop_stmt.append(each)
        if re.search(alter_stmt_change_regx,each):
            alter_change_stmt.append(each)

        insert_reg_found = re.search(insert_stmt_regx,each)
        if insert_reg_found and insert_reg_found.group(1) not in exceptional_tables:
            insert_set_stmts.append(each)
    return drop_statements,alter_add_stmt,alter_drop_stmt,alter_change_stmt,insert_set_stmts



def write_sql_to_file(output_file, tables_dict, drop_statements,alter_add_stmt,alter_drop_stmt,alter_change_stmt,insert_set_stmts):

    for each in tables_dict:
        if tables_dict[each]:
            output_file.write(tables_dict[each])
    for each in drop_statements:
        output_file.write(each)
    for each in alter_drop_stmt:
        output_file.write(each)
    for each in alter_add_stmt:
        output_file.write(each)
    for each in alter_change_stmt:
        output_file.write(each)
    for each in insert_set_stmts:
        output_file.write(each)

    output_file.close()


if __name__ == "__main__":
    INPUT_FILE_PATH = sys.argv[1]
    OUTPUT_FILE_PATH = sys.argv[2]	

    file = open(INPUT_FILE_PATH, 'r')	
    create_table_stmts = get_create_table_statements(file)

    create_regx = r'CREATE TABLE (.*) \((.*)'
    fk_regex = 'CONSTRAINT (.*) FOREIGN KEY (.*) REFERENCES (.*)'
    tables_dict = sort_create_table_stmts(create_table_stmts, create_regx, fk_regex)

    file = open(INPUT_FILE_PATH, 'r')
    exceptional_tables = ['`auth_permission`','`auth_user`','`auth_userprofile`']
    drop_stmt_regx = 'DROP TABLE (.*)'
    alter_stmt_add_regx = 'ALTER TABLE (.*) ADD (.*)'
    alter_stmt_drop_regx = 'ALTER TABLE (.*) DROP (.*)'
    alter_stmt_change_regx =  'ALTER TABLE (.*) CHANGE (.*)'
    insert_stmt_regx = 'UPDATE (.*) SET (.*)'
    drop_statements, alter_add_stmt, alter_drop_stmt, alter_change_stmt, insert_set_stmts = segrigate_statements(file, drop_stmt_regx, alter_stmt_add_regx, alter_stmt_drop_regx, alter_stmt_change_regx, insert_stmt_regx, exceptional_tables)

    output_file = open(OUTPUT_FILE_PATH, 'a')
    write_sql_to_file(output_file, tables_dict,drop_statements,alter_add_stmt,alter_drop_stmt,alter_change_stmt,insert_set_stmts)
