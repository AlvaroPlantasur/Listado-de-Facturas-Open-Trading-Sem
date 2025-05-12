import os
import psycopg2
from openpyxl import load_workbook
# from openpyxl.styles import Font # No se usa activamente
from openpyxl.utils import get_column_letter
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sys

def main():
    # 1. Obtener credenciales y la ruta del archivo base
    db_name = os.environ.get('DB_NAME')
    db_user = os.environ.get('DB_USER')
    db_password = os.environ.get('DB_PASSWORD')
    db_host = os.environ.get('DB_HOST')
    db_port = os.environ.get('DB_PORT')
    file_path = os.environ.get('EXCEL_FILE_PATH')
    
    db_params = {
        'dbname': db_name,
        'user': db_user,
        'password': db_password,
        'host': db_host,
        'port': db_port,
        'sslmode': 'require'
    }
    
     # 2. Definir la nueva consulta SQL con fechas dinámicas
    fecha_inicio_str = '2025-01-01'
    fecha_fin = datetime.now().date()
    fecha_fin_str = fecha_fin.strftime('%Y-%m-%d')
    
    print(f"Rango de fechas para la consulta: Desde {fecha_inicio_str} hasta {fecha_fin_str}")

    # 3. Consulta SQL (modificada para que "FECHA FACTURA" sea un tipo de dato de fecha)
    query = f"""
    SELECT 
    rc.name as "Compañía",
    a.internal_number as "Número",
    rp.vat as "CIF",
    p.name AS "Cliente/Proveedor",
	CASE 
    WHEN a.type = 'out_invoice' THEN 'Cliente'
    WHEN a.type = 'in_invoice' THEN 'Proveedor'
    WHEN a.type = 'out_refund' THEN 'Rectificativa Cliente'
    WHEN a.type = 'in_refund' THEN 'Rectificativa Proveedor'
    ELSE 'Otro'
    END as "Tipo",
 
    EXTRACT(DAY FROM a.date_invoice) AS "Fecha Dia",
	EXTRACT(MONTH FROM a.date_invoice) AS "Fecha Mes",
	EXTRACT(YEAR FROM a.date_invoice) AS "Fecha Año",
	TO_CHAR(a.date_invoice, 'DD/MM/YYYY') AS "Fecha",
 
    CASE 
        WHEN afp.name = 'Recargo de Equivalencia' THEN 'Recargo de Equivalencia'
        WHEN afp.name = 'Régimen Extracomunitario' THEN 'Régimen Extracomunitario'
        WHEN afp.name IN ('REGIMEN INTRACOMUNITARIO', 'Régimen Intracomunitario') THEN 'Régimen Intracomunitario'
        WHEN afp.name = 'REGIMEN NACIONAL' THEN 'Régimen Nacional'
        ELSE afp.name
    END as "Régimen fiscal",
 
    a.amount_untaxed_0 as "Base 0", 
    a.amount_taxed_0 as "Importe 0", 
    a.amount_total_0 as "Total 0",
 
    a.amount_untaxed_4 as "Base 4", 
    a.amount_taxed_4 as "Importe 4", 
    a.amount_total_4 as "Total 4",
 
    a.amount_untaxed_8 as "Base 8", 
    a.amount_taxed_8 as "Importe 8", 
    a.amount_total_8 as "Total 8",
 
    a.amount_untaxed_10 as "Base 10", 
    a.amount_taxed_10 as "Importe 10", 
    a.amount_total_10 as "Total 10",
 
    a.amount_untaxed_18 as "Base 18", 
    a.amount_taxed_18 as "Importe 18", 
    a.amount_total_18 as "Total 18",
 
    a.amount_untaxed_21 as "Base 21", 
    a.amount_taxed_21 as "Importe 21", 
    a.amount_total_21 as "Total 21",
 
    a.recargo_equivalencia_05 as "Recargo 0,5", 
    a.recargo_equivalencia_1 as "Recargo 1", 
    a.recargo_equivalencia_14 as "Recargo 1.4", 
    a.recargo_equivalencia_4 as "Recargo 4",
    a.recargo_equivalencia_52 as "Recargo 5.2",
 
    a.amount_total as "Total",
 
  
	(CASE 							    -- Total Bases (suma de todas las bases) con signo negativo en 'Rectificativa Cliente'
        WHEN a.type = 'out_refund' THEN -- Si es rectificativa cliente
            - (
                COALESCE(a.amount_untaxed_0, 0) +
                COALESCE(a.amount_untaxed_4, 0) +
                COALESCE(a.amount_untaxed_8, 0) +
                COALESCE(a.amount_untaxed_10, 0) +
                COALESCE(a.amount_untaxed_18, 0) +
                COALESCE(a.amount_untaxed_21, 0)
            )
        ELSE -- Para otros tipos de factura, el valor permanece positivo
            (
                COALESCE(a.amount_untaxed_0, 0) +
                COALESCE(a.amount_untaxed_4, 0) +
                COALESCE(a.amount_untaxed_8, 0) +
                COALESCE(a.amount_untaxed_10, 0) +
                COALESCE(a.amount_untaxed_18, 0) +
                COALESCE(a.amount_untaxed_21, 0)
            )
    	END) as "Total BASES",
    CASE   
        WHEN a.obsolescencia = TRUE THEN 'SI' 
        ELSE 'NO' 
    END as "BULK",  -- Columna Obsolescencia (Bulk)
	'S-' || rp.id AS "ID Cliente",
	rpa.city AS "Ciudad"
 
FROM 
    account_invoice as a 
INNER JOIN 
    res_partner as p ON p.id = a.partner_id 
INNER JOIN 
    account_fiscal_position afp ON afp.id = a.fiscal_position 
INNER JOIN 
    res_company rc ON rc.id = a.company_id
INNER JOIN 
    res_partner rp ON rp.id = a.partner_id
INNER JOIN 
	res_partner_address rpa ON rpa.id = a.address_invoice_id
 
WHERE 
    a.state IN ('open','paid') 
    AND a.date_invoice BETWEEN '2025-04-01' AND '2025-04-05'
    AND a.type IN ('out_invoice', 'out_refund') -- Solo facturas y rectificativas de clientes
	AND a.internal_number NOT LIKE 'RA%' -- Omitir RAPPEL
 
ORDER BY 
    a.internal_number;
    """
    
    # 4. Ejecutar la consulta y obtener los datos
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                resultados = cur.fetchall()
                headers = [desc[0] for desc in cur.description]
    except Exception as e:
        print(f"Error al conectar o ejecutar la consulta: {e}")
        sys.exit(1)
    
    if not resultados:
        print("No se obtuvieron resultados de la consulta.")
        return
    else:
        print(f"Se obtuvieron {len(resultados)} filas de la consulta.")
    
    # 5. Abrir el archivo base
    try:
        book = load_workbook(file_path)
        sheet = book.active
    except FileNotFoundError:
        print(f"No se encontró el archivo base '{file_path}'. Se aborta para no perder el formato.")
        return
    
        # 6. Eliminar el contenido existente desde la segunda fila (mantiene encabezados y formatos)
    for row in sheet.iter_rows(min_row=2, max_row=sheet.max_row):
        for cell in row:
            cell.value = None

    # 7. Insertar nuevas filas desde la segunda fila
    start_row = 2
    for row_idx, row_data in enumerate(resultados, start=start_row):
        for col_idx, value in enumerate(row_data, start=1):
            sheet.cell(row=row_idx, column=col_idx, value=value)
    
    print(f"Se sobrescribieron {len(resultados)} filas en la hoja.")

    
    # 8. Guardar el libro
    try:
        book.save(file_path)
        print(f"Archivo guardado con los datos actualizados en '{file_path}'.")
    except Exception as e:
        print(f"Error al guardar el archivo '{file_path}': {e}")

if __name__ == '__main__':
    main()
