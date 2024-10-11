SELECT COUNT(e.employee_id)
FROM employees e
JOIN departments d ON d.department_id = e.department_id 
JOIN locations l ON d.location_id = l.location_id
JOIN countries c ON c.country_id = l.country_id
JOIN regions r ON c.region_id = r.region_id 
WHERE r.region_name = 'Europe';
