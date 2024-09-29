{% macro date_difference_with_weekend_adjustment(relation, start_date, end_date, unit) %}
    {# SQL fonksiyonlarını kullanarak start ve end tarihlerini haftaiçi günlere göre ayarlıyoruz #}
    
    {% set start_dt = "CAST(" ~ start_date ~ " AS DATETIME)" %}
    {% set end_dt = "CAST(" ~ end_date ~ " AS DATETIME)" %}

    {# Başlangıç tarihini hafta içine ayarlama #}
    {% set adjusted_start_dt = """
        CASE
            WHEN DATEPART(WEEKDAY, {{ start_dt }}) = 7 THEN DATEADD(DAY, 1, CAST({{ start_dt }} AS DATETIME)) -- Cumartesi ise Pazar
            WHEN DATEPART(WEEKDAY, {{ start_dt }}) = 1 THEN DATEADD(DAY, 1, CAST({{ start_dt }} AS DATETIME)) -- Pazar ise Pazartesi
            ELSE {{ start_dt }}
        END
    """ %}

    {# Bitiş tarihini hafta içine ayarlama #}
    {% set adjusted_end_dt = """
        CASE
            WHEN DATEPART(WEEKDAY, {{ end_dt }}) = 7 THEN DATEADD(DAY, -1, CAST({{ end_dt }} AS DATETIME)) -- Cumartesi ise bir önceki Cuma
            WHEN DATEPART(WEEKDAY, {{ end_dt }}) = 1 THEN DATEADD(DAY, -2, CAST({{ end_dt }} AS DATETIME)) -- Pazar ise bir önceki Cuma
            ELSE {{ end_dt }}
        END
    """ %}

    {# Toplam iş saatlerini hesaplama (9:00 - 18:00 arası) #}
    {% set total_seconds_sql = """
        DATEDIFF(SECOND, 
            CASE
                WHEN CAST({{ adjusted_start_dt }} AS TIME) < '09:00' THEN CAST({{ adjusted_start_dt }} AS DATE) + '09:00'
                WHEN CAST({{ adjusted_start_dt }} AS TIME) > '18:00' THEN CAST({{ adjusted_start_dt }} AS DATE) + '18:00'
                ELSE {{ adjusted_start_dt }}
            END,
            CASE
                WHEN CAST({{ adjusted_end_dt }} AS TIME) < '09:00' THEN CAST({{ adjusted_end_dt }} AS DATE) + '09:00'
                WHEN CAST({{ adjusted_end_dt }} AS TIME) > '18:00' THEN CAST({{ adjusted_end_dt }} AS DATE) + '18:00'
                ELSE {{ adjusted_end_dt }}
            END
        )
    """ %}

    {# Sonuçları istenilen birime çevirme #}
    {% if unit == 'seconds' %}
        {{ total_seconds_sql }}
    {% elif unit == 'minutes' %}
        {{ total_seconds_sql }} / 60
    {% elif unit == 'hours' %}
        {{ total_seconds_sql }} / 3600
    {% endif %}
{% endmacro %}
