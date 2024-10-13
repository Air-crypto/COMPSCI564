SELECT COUNT(e.employee_id) 
FROM employees e
JOIN departments d ON d.department_id = e.department_id 
WHERE d.department_name = 'Shipping';