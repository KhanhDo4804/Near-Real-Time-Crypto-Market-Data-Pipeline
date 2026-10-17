{% macro mark_loaded_files(file_paths) %}

    {% if file_paths | length > 0 %}

        {% set sql %}
            BEGIN;

            CREATE TEMP TABLE files_to_mark (
                file_path VARCHAR(2048)
            );

            INSERT INTO files_to_mark (file_path)
            VALUES
                {% for path in file_paths %}
                    ('{{ path | replace("'", "''") }}')
                    {% if not loop.last %},{% endif %}
                {% endfor %}
            ;

            INSERT INTO etl.loaded_files (file_path)
            SELECT DISTINCT new_files.file_path
            FROM files_to_mark AS new_files
            WHERE NOT EXISTS (
                SELECT 1
                FROM etl.loaded_files AS loaded
                WHERE loaded.file_path = new_files.file_path
            );

            DROP TABLE files_to_mark;

            COMMIT;
        {% endset %}

        {% do run_query(sql) %}

    {% endif %}

{% endmacro %}