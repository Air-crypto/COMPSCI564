SELECT d.department_name
FROM employees e
JOIN departments d ON e.department_id = d.department_id
WHERE e.salary = (SELECT MAX(salary) FROM employees);

