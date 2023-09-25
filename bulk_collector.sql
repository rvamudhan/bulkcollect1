BULK COLLECTOR | BULK COLLET | FOR ALL (BULK BIND) | LIMIT (BULK_EXCEPTIONS)
- Used to process large volume of data.
- Mainly used to reduce context switch in oracle.
- Huge volume of data should be updated / deleted/ inserted - BULK COLLECTOR
- Used to process large volume of data.
-- execution map  - when we execute SQL within PLSQL 
			PLSQL - SQL (execution) - SQL ENGINE - PLSQL ENGINE 
			This switching between sql and plsql is called context switching. This is reduced by using bulk collector.
bulk collect - fetch & select statement

--TASK 1( using for loop)
CREATE TABLE bulk_employees_tb
AS
SELECT first_name, salary, department_id from employees 
WHERE 1 = 2;

DECLARE 
TYPE name_typ is TABLE OF VARCHAR2(30);
TYPE salary_typ is TABLE OF NUMBER;
TYPE did_typ IS TABLE OF NUMBER;

v_first_name name_typ := name_typ();
v_salary salary_typ := salary_typ();
v_department_id did_typ := did_typ();

CURSOR emp_c IS
SELECT first_name, salary, department_id 
FROM employees;
BEGIN
EXECUTE IMMEDIATE 'TRUNCATE TABLE bulk_employees_tb';
OPEN emp_c;
FETCH emp_c BULK COLLECT INTO 
v_first_name, v_salary, v_department_id;
CLOSE emp_c ;
FOR i IN v_first_name.FIRST..v_first_name.COUNT
LOOP
INSERT INTO bulk_employees_tb VALUES 
(v_first_name(i), v_salary(i), v_department_id(i));
dbms_output.put_line('Rows inserted ='||i);
end LOOP;
end;


--TASK 2 (using forall)

DECLARE 
TYPE name_typ is TABLE OF VARCHAR2(30);
TYPE salary_typ is TABLE OF NUMBER;
TYPE did_typ IS TABLE OF NUMBER;

v_first_name name_typ := name_typ();
v_salary salary_typ := salary_typ();
v_department_id did_typ := did_typ();

CURSOR emp_c IS
SELECT first_name, salary, department_id 
FROM employees;
BEGIN
EXECUTE IMMEDIATE 'TRUNCATE TABLE bulk_employees_tb';
OPEN emp_c;
FETCH emp_c BULK COLLECT INTO 
v_first_name, v_salary, v_department_id;
CLOSE emp_c ;
FORALL i IN v_first_name.FIRST..v_first_name.COUNT
INSERT INTO bulk_employees_tb VALUES 
(v_first_name(i), v_salary(i), v_department_id(i));
dbms_output.put_line('Rows inserted ='||sql%rowcount);
end;


--Task 3 ( using limit)

ALTER TABLE bulk_employees_tb
ADD employee_id NUMBER;
ALTER TABLE bulk_employees_tb
DROP CONSTRAINT eid_ck;
ALTER table bulk_employees_tb
ADD CONSTRAINT eid_ck check( employee_id NOT IN (120,145,179,197,205));



DECLARE 
TYPE emp_typ IS TABLE OF NUMBER;
TYPE name_typ is TABLE OF VARCHAR2(30);
TYPE salary_typ is TABLE OF NUMBER;
TYPE did_typ IS TABLE OF NUMBER;

v_employee_id emp_typ := emp_typ();
v_first_name name_typ := name_typ();
v_salary salary_typ := salary_typ();
v_department_id did_typ := did_typ();
v_err_cnt NUMBER;

CURSOR emp_c IS
SELECT employee_id, first_name, salary, department_id 
FROM employees;
BEGIN
EXECUTE IMMEDIATE 'TRUNCATE TABLE bulk_employees_tb';
OPEN emp_c;
LOOP
FETCH emp_c BULK COLLECT INTO 
v_employee_id, v_first_name, v_salary, v_department_id LIMIT 30;
BEGIN
FORALL i IN v_first_name.first..v_first_name.COUNT save EXCEPTIONS
INSERT /*+append*/ INTO bulk_employees_tb VALUES (v_employee_id(i), v_first_name(i), v_salary(i), v_department_id(i));
--dbms_output.put_line('Rows inserted ='||sql%rowcount);

EXCEPTION
WHEN OTHERS THEN
dbms_output.put_line('Rows inserted ='||sql%rowcount);
v_err_cnt := sql%bulk_exceptions.count;
FOR i IN 1 .. v_err_cnt
LOOP
dbms_output.put_line (' *_*_*_*_*_*_*_*_*_*_*');
dbms_output.put_line ('Error index' || sql%bulk_exceptions(i).error_index);
dbms_output.put_line('Error code' || sql%bulk_exceptions(i).ERROR_CODE|| 'Error--' || SQLERRM);
END LOOP;
dbms_output.put_line (' *_*_*_*_*_*_*_*_*_*_*');
end;
EXIT when emp_c%notfound;
end LOOP;
CLOSE emp_c;
COMMIT;
end;
/