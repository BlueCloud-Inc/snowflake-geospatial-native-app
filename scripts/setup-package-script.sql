-- ################################################################
-- Create SHARED_CONTENT_SCHEMA to share in the application package
-- ################################################################
USE {{ package_name }};
create schema if not exists shared_content_schema;

use schema shared_content_schema;
create or replace view FROSTBYTE_TB_SAFEGRAPH_S as select * from NATIVE_APP_QUICKSTART_DB.NATIVE_APP_QUICKSTART_SCHEMA.FROSTBYTE_TB_SAFEGRAPH_S;

grant usage on schema shared_content_schema to share in application package {{ package_name }};
grant reference_usage on database NATIVE_APP_QUICKSTART_DB to share in application package {{ package_name }};
grant select on view FROSTBYTE_TB_SAFEGRAPH_S to share in application package {{ package_name }}