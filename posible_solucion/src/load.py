from sqlalchemy import create_engine, text 

def load_data(tablas: dict, db_url: str):
    print("Iniciando carga al Data Warehouse...")
    engine = create_engine(db_url)
    
    try:
        # LIMPIEZA DE TABLAS 
        # Vaciamos las tablas antes de cargar para evitar el error de "llave duplicada"
        print("Vaciando datos anteriores (TRUNCATE) para mantener el esquema limpio...")
        with engine.begin() as conn:
            conn.execute(text("""
                TRUNCATE TABLE fact_matriculas, dim_institucion, dim_programa, 
                dim_ubicacion, dim_tiempo, dim_demografia CASCADE;
            """))
    
        # 1. Dimensiones primero
        print("Cargando dimensiones...")
        tablas['dim_institucion'].to_sql('dim_institucion', engine, if_exists='append', index=False)
        tablas['dim_programa'].to_sql('dim_programa', engine, if_exists='append', index=False)
        tablas['dim_ubicacion'].to_sql('dim_ubicacion', engine, if_exists='append', index=False)
        tablas['dim_tiempo'].to_sql('dim_tiempo', engine, if_exists='append', index=False)
        tablas['dim_demografia'].to_sql('dim_demografia', engine, if_exists='append', index=False)
        
        # 2. Hechos al final
        print("Cargando tabla de hechos...")
        tablas['fact_matriculas'].to_sql('fact_matriculas', engine, if_exists='append', index=False)
        
        print("¡Carga completada exitosamente sobre el esquema existente!")
        
    except Exception as e:
        print(f"Error durante la carga: {e}")
        raise e