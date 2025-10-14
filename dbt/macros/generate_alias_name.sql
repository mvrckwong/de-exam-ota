{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- if custom_alias_name is none -%}
        {%- if node.resource_type == 'seed' -%}
            {# Split by both forward slash and backslash #}
            {%- set path_parts = node.original_file_path.replace('\\', '/').split('/') -%}
            
            {# Check if we have a folder structure (more than just seeds/file.csv) #}
            {%- if path_parts | length > 2 -%}
                {# Get folder name (second to last element) #}
                {%- set folder_name = path_parts[-2] -%}
                {# Get filename (last element, without .csv) #}
                {%- set file_name = node.name -%}
                
                {# Combine folder_filename #}
                {{ folder_name ~ '_' ~ file_name }}
            {%- else -%}
                {# No subfolder, just use filename #}
                {{ node.name }}
            {%- endif -%}
        {%- else -%}
            {# Not a seed, use default behavior #}
            {{ node.name }}
        {%- endif -%}
    {%- else -%}
        {# Custom alias provided #}
        {{ custom_alias_name | trim }}
    {%- endif -%}
{%- endmacro %}