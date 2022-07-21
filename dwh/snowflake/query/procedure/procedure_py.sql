/* stored procedure in python */

-- snowpark version
select * from information_schema.packages
where language = 'python';

select * from information_schema.packages
where language = 'python'
and package_name = 'snowflake-snowpark-python'
order by version desc;

-- in line
CREATE OR REPLACE PROCEDURE PYROCEDURE(from_table STRING, to_table STRING, cnt INT)
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.8'
  PACKAGES = ('snowflake-snowpark-python')
  HANDLER = 'run'
AS
$$
def run(session, from_table, to_table, cnt):
    (
      session.table(from_table).limit(cnt)
      .write.mode('overwrite')
      .save_as_table(to_table)
    )
    return 'OK!'
$$;

CALL pyrocedure('debit', 'credit', 3);


-- stage upload
/* pyrocedure.py

def run(session, from_table, to_table, count):
  session.table(from_table).limit(count).write.save_as_table(to_table)
  return 'OK!'

CREATE OR REPLACE STAGE pystage;
PUT file:///tmp/files/pyrocedure.py @pystage
AUTO_COMPRESS = FALSE
OVERWRITE = TRUE;
LIST @pystage;
*/

CREATE OR REPLACE PROCEDURE PYROCEDURE(from_table STRING, to_table STRING, count INT)
  RETURNS INT
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.8'
  PACKAGES = ('snowflake-snowpark-python')
  IMPORTS = ('@pystage/pyrocedure.py')
  HANDLER = 'pyrocedure.run';
