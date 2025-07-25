{{ config(materialized='table', schema='gold', alias='teste_do_ihas')}}

SELECT 
	"DATA",
	"DESTINO",
	"VALOR"
FROM
    {{ source('bronze', 'google_sheets_Pagina1') }}
