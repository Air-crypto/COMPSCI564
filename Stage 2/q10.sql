SELECT employee_id
FROM employees e
WHERE NOT EXISTS (
    SELECT 1 FROM dependents d WHERE d.employee_id = e.employee_id
);
