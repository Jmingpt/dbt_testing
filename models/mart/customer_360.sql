select
    r.customer_id,
    round(sum(p.amount),2) as revenue,
    count(distinct r.rental_id) as rental_count,
    date_diff(current_date, max(rental_date), day) as recency,
    count(case when r.is_late_return = 'Late' then r.rental_id end) as late_rentals,
    min(p.amount) as min_payment,
    max(p.amount) as max_payment,
    round(avg(p.amount),2) as avg_payment

from {{ ref('rentals_denormalized') }} as r, unnest(payments) as p
group by 1