SELECT employee_id
FROM employees e
WHERE NOT EXISTS (
    SELECT 1 FROM dependents d WHERE d.employee_id = e.employee_id
);

-- using left join
SELECT e.employee_id
FROM employees e
LEFT JOIN dependents d ON e.employee_id = d.employee_id
WHERE d.dependent_id IS NULL;
