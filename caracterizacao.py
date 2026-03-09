# =============================================================================
# CARACTERIZAÇÃO DO PROJECTO DE CAMPANHAS
# =============================================================================
# Script autónomo que caracteriza:
#   1. Plano de campanhas a data de hoje
#   2. Escassez das campanhas (escassas vs não escassas)
#   3. Base de clientes por Tier de antiguidade
#   4. Base de clientes por Pressão Comercial (0, 1, 2, 3, >3)
#   5. Análise cruzada Tier × Pressão Comercial
#   6. Visão geral do projecto (totais, distribuição temporal, volumetria)
#
# Execução diária: usa datetime.now() como referência temporal.
# Requer: df_plano.csv no PATH_PLANO e (opcionalmente) ligação SQL Server.
# Se a ligação SQL não estiver disponível, produz a caracterização com os dados
# do CSV e assinala as secções que necessitam de dados da base de dados.
# =============================================================================

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

# =============================================================================
# CONFIGURAÇÃO
# =============================================================================
PATH_PLANO   = r'C:\Users\COTAGO\Desktop\Base\plano.csv'
SEPARADOR    = ';'
ENCODING     = 'utf-8-sig'

# Thresholds de escassez (consistentes com o motor de optimização)
ESCASSEZ_THRESHOLD_CRITICA = 0.80   # Acima disto: campanha CRÍTICA (escassa)
ESCASSEZ_THRESHOLD_ALTA    = 0.50
ESCASSEZ_THRESHOLD_MEDIA   = 0.30

# Data de referência
hoje      = datetime.now()
hoje_str  = hoje.strftime('%d/%m/%Y')
mes_atual = int(hoje.strftime('%Y%m'))

# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

def calcular_tier_antiguidade(idmes_ultima_campanha, mes_atual):
    """
    Classifica um cliente pelo tempo sem receber campanha.

    Tier 1 (NULL / nunca contactado) → MÁXIMA PRIORIDADE
    Tier 2 (≥ 6 meses)               → ALTA PRIORIDADE
    Tier 3 (3 – 5 meses)             → MÉDIA PRIORIDADE
    Tier 4 (< 3 meses)               → BAIXA PRIORIDADE
    """
    if (
        pd.isna(idmes_ultima_campanha)
        or str(idmes_ultima_campanha).strip() in ('', 'NULL', 'None', 'nan', 'NaT')
    ):
        return 1

    try:
        idmes = int(idmes_ultima_campanha)
        ano_atual       = mes_atual // 100
        mes_atual_num   = mes_atual % 100
        ano_ultimo      = idmes   // 100
        mes_ultimo      = idmes   %  100
        meses_atras     = (ano_atual - ano_ultimo) * 12 + (mes_atual_num - mes_ultimo)

        if   meses_atras >= 6: return 2
        elif meses_atras >= 3: return 3
        else:                  return 4
    except Exception:
        return 3   # fallback: tier médio se não for possível interpretar


def label_tier(t):
    return {
        1: 'Tier 1 – Nunca contactado (NULL)',
        2: 'Tier 2 – ≥ 6 meses sem campanha',
        3: 'Tier 3 – 3 a 5 meses sem campanha',
        4: 'Tier 4 – < 3 meses sem campanha',
    }.get(t, f'Tier {t}')


def label_pressao(p):
    if   p == 0: return '0 – Sem pressão'
    elif p == 1: return '1 campanha'
    elif p == 2: return '2 campanhas'
    elif p == 3: return '3 campanhas'
    else:        return '> 3 campanhas'


def nivel_escassez(e):
    if   e > ESCASSEZ_THRESHOLD_CRITICA: return 'CRÍTICO'
    elif e > ESCASSEZ_THRESHOLD_ALTA:    return 'ALTO'
    elif e > ESCASSEZ_THRESHOLD_MEDIA:   return 'MÉDIO'
    else:                                return 'BAIXO'


def sep(char='=', width=80):
    print(char * width)


def titulo(texto, char='='):
    sep(char)
    print(f'  {texto}')
    sep(char)


# =============================================================================
# CARREGAMENTO DO PLANO (CSV)
# =============================================================================
def carregar_plano(path=PATH_PLANO):
    """Carrega df_plano a partir do CSV, com fallback para o ficheiro local."""
    _script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in dir() else os.getcwd()
    caminhos = [path, os.path.join(_script_dir, 'df_plano.csv'), 'df_plano.csv']
    for c in caminhos:
        if os.path.exists(c):
            df = pd.read_csv(c, sep=SEPARADOR, encoding=ENCODING)
            df.columns = [col.strip() for col in df.columns]
            df['data']       = pd.to_datetime(df['data'], dayfirst=True, errors='coerce')
            df['volumetria'] = pd.to_numeric(df['volumetria'], errors='coerce').fillna(0).astype(int)
            if 'tipo oferta' not in df.columns:
                df['tipo oferta'] = 'PCD'
            return df
    raise FileNotFoundError(
        f"Ficheiro de plano não encontrado em {path} nem em df_plano.csv"
    )


# =============================================================================
# LIGAÇÃO SQL (OPCIONAL)
# =============================================================================
def obter_base_envio():
    """
    Tenta ligar ao SQL Server e carregar a base de envio.
    Devolve None se a ligação não estiver disponível.
    """
    try:
        import pyodbc
    except ImportError:
        print("  ⚠  Módulo pyodbc não instalado – sem acesso SQL.")
        print("     Secções de caracterização de clientes omitidas.\n")
        return None

    try:
        conn = pyodbc.connect(
            'Driver={SQL Server};'
            'Server=Diomedes;'
            'Database=tempdb;'
            'Trusted_Connection=yes;'
        )
        df = pd.read_sql("SELECT DISTINCT * FROM #BaseEnvio", conn)
        conn.close()
        return df
    except Exception as e:
        print(f"  ⚠  Sem acesso à base de dados SQL ({e.__class__.__name__}: {e}).")
        print("     Secções de caracterização de clientes omitidas.\n")
        return None


# =============================================================================
# SECÇÃO 1 – VISÃO GERAL DO PROJECTO (PLANO COMPLETO)
# =============================================================================
def caracterizar_plano(df_plano):
    titulo('1. VISÃO GERAL DO PROJECTO – PLANO COMPLETO')

    total_camps   = len(df_plano)
    total_vol     = df_plano['volumetria'].sum()
    camps_passadas = df_plano[df_plano['data'].dt.date <  hoje.date()]
    camps_hoje     = df_plano[df_plano['data'].dt.date == hoje.date()]
    camps_futuras  = df_plano[df_plano['data'].dt.date >  hoje.date()]

    print(f"  Data de execução      : {hoje.strftime('%Y-%m-%d')}")
    print(f"  Total de campanhas    : {total_camps}")
    print(f"    • Passadas          : {len(camps_passadas)}")
    print(f"    • Hoje              : {len(camps_hoje)}")
    print(f"    • Futuras           : {len(camps_futuras)}")
    print(f"  Volume total planeado : {total_vol:,} contactos")
    print()

    # Distribuição por Tipo de Oferta
    print("  Distribuição por Tipo de Oferta:")
    tp = (
        df_plano.groupby('tipo oferta', dropna=False)
        .agg(n_campanhas=('campanha', 'count'), volumetria=('volumetria', 'sum'))
        .sort_values('n_campanhas', ascending=False)
    )
    for tipo, row in tp.iterrows():
        pct = row['volumetria'] / total_vol * 100 if total_vol else 0
        print(f"    {str(tipo):<30}  {row['n_campanhas']:>3} campanha(s)   {row['volumetria']:>8,} contactos  ({pct:.1f}%)")
    print()

    # Distribuição temporal (por semana/data)
    print("  Distribuição temporal (por data de envio):")
    dist_data = (
        df_plano.groupby(df_plano['data'].dt.date)
        .agg(n=('campanha', 'count'), vol=('volumetria', 'sum'))
        .sort_index()
    )
    for data, row in dist_data.iterrows():
        marcador = '  ◄ HOJE' if data == hoje.date() else ''
        print(f"    {str(data)}  {row['n']:>3} campanha(s)   {row['vol']:>8,} contactos{marcador}")
    print()


# =============================================================================
# SECÇÃO 2 – CAMPANHAS DE HOJE
# =============================================================================
def caracterizar_campanhas_hoje(df_plano, df_base=None):
    titulo('2. CAMPANHAS A DATA DE HOJE')

    camps_hoje = df_plano[df_plano['data'].dt.date == hoje.date()].copy()

    if camps_hoje.empty:
        print("  ℹ  Não existem campanhas agendadas para hoje.\n")
        return camps_hoje

    total_vol_hoje = camps_hoje['volumetria'].sum()
    print(f"  Total de campanhas hoje : {len(camps_hoje)}")
    print(f"  Volume total hoje       : {total_vol_hoje:,} contactos")
    print()

    # Detalhes por campanha (com escassez se df_base disponível)
    print(f"  {'Campanha':<35} {'Tipo':<20} {'Volumetria':>12}  Escassez  Nível")
    print(f"  {'-'*35} {'-'*20} {'-'*12}  {'-'*8}  {'-'*8}")

    for _, row in camps_hoje.sort_values('volumetria', ascending=False).iterrows():
        camp = row['campanha']
        vol  = int(row['volumetria'])
        tipo = str(row.get('tipo oferta', 'PCD'))

        if df_base is not None and camp in df_base.columns:
            elegiveis = int((df_base[camp] == 1).sum())
            escassez  = vol / max(elegiveis, 1)
            nivel     = nivel_escassez(escassez)
            esc_str   = f'{escassez:.2f}'
        else:
            elegiveis = None
            escassez  = None
            nivel     = 'N/D'
            esc_str   = 'N/D'

        print(f"  {camp:<35} {tipo:<20} {vol:>12,}  {esc_str:>8}  {nivel}")

    print()
    return camps_hoje


# =============================================================================
# SECÇÃO 3 – ESCASSEZ DE TODAS AS CAMPANHAS DO PLANO
# =============================================================================
def caracterizar_escassez(df_plano, df_base=None):
    titulo('3. CARACTERIZAÇÃO DE ESCASSEZ DAS CAMPANHAS')

    if df_base is None:
        print("  ⚠  Sem dados de clientes: escassez calculável apenas com elegibilidade real.")
        print("     A seguir apresenta-se a volumetria do plano como referência.\n")
        _mostrar_volumetria_plano(df_plano)
        return

    resultados = []
    for _, row in df_plano.iterrows():
        camp = row['campanha']
        vol  = int(row['volumetria'])
        data = row['data']

        if camp in df_base.columns:
            elegiveis = int((df_base[camp] == 1).sum())
        else:
            elegiveis = 0

        escassez = vol / max(elegiveis, 1)
        resultados.append({
            'campanha'  : camp,
            'data'      : data,
            'tipo'      : str(row.get('tipo oferta', '')),
            'volumetria': vol,
            'elegiveis' : elegiveis,
            'escassez'  : escassez,
            'nivel'     : nivel_escassez(escassez),
            'passada'   : data.date() < hoje.date() if pd.notna(data) else False,
        })

    df_esc = pd.DataFrame(resultados)

    # Resumo por nível
    contagem = df_esc['nivel'].value_counts().reindex(
        ['CRÍTICO', 'ALTO', 'MÉDIO', 'BAIXO'], fill_value=0
    )
    escassas     = contagem['CRÍTICO'] + contagem['ALTO']
    nao_escassas = contagem['MÉDIO']   + contagem['BAIXO']

    print(f"  Threshold escassez crítica : > {ESCASSEZ_THRESHOLD_CRITICA:.0%}")
    print(f"  Threshold escassez alta    : > {ESCASSEZ_THRESHOLD_ALTA:.0%}")
    print()
    print(f"  Campanhas ESCASSAS   (CRÍTICO + ALTO) : {escassas:>4}")
    print(f"    • CRÍTICO  (> {ESCASSEZ_THRESHOLD_CRITICA:.0%})              : {contagem['CRÍTICO']:>4}")
    print(f"    • ALTO     (> {ESCASSEZ_THRESHOLD_ALTA:.0%})              : {contagem['ALTO']:>4}")
    print(f"  Campanhas NÃO ESCASSAS (MÉDIO + BAIXO): {nao_escassas:>4}")
    print(f"    • MÉDIO    (> {ESCASSEZ_THRESHOLD_MEDIA:.0%})              : {contagem['MÉDIO']:>4}")
    print(f"    • BAIXO    (≤ {ESCASSEZ_THRESHOLD_MEDIA:.0%})              : {contagem['BAIXO']:>4}")
    print()

    # Tabela detalhada ordenada por escassez
    df_detalhe = df_esc.sort_values('escassez', ascending=False)
    print(f"  {'Campanha':<35} {'Data':<12} {'Tipo':<20} {'Vol':>8}  {'Elegiveis':>10}  {'Escassez':>9}  Nível")
    print(f"  {'-'*35} {'-'*12} {'-'*20} {'-'*8}  {'-'*10}  {'-'*9}  {'-'*8}")
    for _, r in df_detalhe.iterrows():
        data_str = r['data'].strftime('%Y-%m-%d') if pd.notna(r['data']) else 'N/D'
        print(
            f"  {r['campanha']:<35} {data_str:<12} {r['tipo']:<20} "
            f"{r['volumetria']:>8,}  {r['elegiveis']:>10,}  {r['escassez']:>9.2f}  {r['nivel']}"
        )
    print()


def _mostrar_volumetria_plano(df_plano):
    """Fallback: mostra volumetria quando não há dados de elegibilidade."""
    for _, row in df_plano.sort_values('volumetria', ascending=False).iterrows():
        data_str = row['data'].strftime('%Y-%m-%d') if pd.notna(row['data']) else 'N/D'
        print(f"  {row['campanha']:<35} {data_str:<12} {int(row['volumetria']):>10,}")
    print()


# =============================================================================
# SECÇÃO 4 – CARACTERIZAÇÃO DE CLIENTES POR TIER
# =============================================================================
def caracterizar_clientes_por_tier(df_base):
    titulo('4. CARACTERIZAÇÃO DA BASE DE CLIENTES POR TIER')

    if 'IdMesUltimaCampanha' not in df_base.columns:
        print("  ⚠  Coluna IdMesUltimaCampanha não encontrada.\n")
        return

    df = df_base.copy()
    df['tier'] = df['IdMesUltimaCampanha'].apply(
        lambda x: calcular_tier_antiguidade(x, mes_atual)
    )

    total = len(df)
    print(f"  Total de clientes na base : {total:,}")
    print()
    print(f"  {'Tier':<40} {'Clientes':>10}  {'%':>8}")
    print(f"  {'-'*40} {'-'*10}  {'-'*8}")
    for t in [1, 2, 3, 4]:
        n   = int((df['tier'] == t).sum())
        pct = n / total * 100 if total else 0
        print(f"  {label_tier(t):<40} {n:>10,}  {pct:>7.1f}%")
    print()

    return df


# =============================================================================
# SECÇÃO 5 – CARACTERIZAÇÃO DE CLIENTES POR PRESSÃO COMERCIAL
# =============================================================================
def caracterizar_clientes_por_pressao(df_base):
    titulo('5. CARACTERIZAÇÃO DA BASE DE CLIENTES POR PRESSÃO COMERCIAL')

    if 'PressaoComercial' not in df_base.columns:
        print("  ⚠  Coluna PressaoComercial não encontrada.\n")
        return

    df = df_base.copy()
    df['pressao_num'] = pd.to_numeric(df['PressaoComercial'], errors='coerce').fillna(0).astype(int)
    df['pressao_grp'] = df['pressao_num'].apply(label_pressao)

    # Ordenação lógica (consistente com label_pressao)
    ordem = [label_pressao(p) for p in [0, 1, 2, 3, 4]]

    total = len(df)
    print(f"  Total de clientes na base : {total:,}")
    print()
    print(f"  {'Pressão Comercial':<30} {'Clientes':>10}  {'%':>8}")
    print(f"  {'-'*30} {'-'*10}  {'-'*8}")
    for grp in ordem:
        n   = int((df['pressao_grp'] == grp).sum())
        pct = n / total * 100 if total else 0
        print(f"  {grp:<30} {n:>10,}  {pct:>7.1f}%")
    print()

    return df


# =============================================================================
# SECÇÃO 6 – ANÁLISE CRUZADA TIER × PRESSÃO COMERCIAL
# =============================================================================
def caracterizar_cruzada_tier_pressao(df_base):
    titulo('6. ANÁLISE CRUZADA: TIER × PRESSÃO COMERCIAL')

    colunas_necessarias = {'IdMesUltimaCampanha', 'PressaoComercial'}
    if not colunas_necessarias.issubset(df_base.columns):
        faltam = colunas_necessarias - set(df_base.columns)
        print(f"  ⚠  Colunas em falta: {faltam}\n")
        return

    df = df_base.copy()
    df['tier'] = df['IdMesUltimaCampanha'].apply(
        lambda x: calcular_tier_antiguidade(x, mes_atual)
    )
    df['pressao_num'] = pd.to_numeric(df['PressaoComercial'], errors='coerce').fillna(0).astype(int)
    df['pressao_grp'] = df['pressao_num'].apply(
        lambda p: '>3' if p > 3 else str(p)   # chaves compactas para pivot
    )

    # Tabela cruzada
    cols_pressao = ['0', '1', '2', '3', '>3']
    tiers        = [1, 2, 3, 4]

    tabela = pd.crosstab(df['tier'], df['pressao_grp'])
    # Garantir todas as colunas
    for c in cols_pressao:
        if c not in tabela.columns:
            tabela[c] = 0
    tabela = tabela[cols_pressao]
    tabela['TOTAL'] = tabela.sum(axis=1)

    total_geral = tabela['TOTAL'].sum()

    header_pressao = '  '.join(f'{c:>10}' for c in cols_pressao)
    print(f"  {'Tier':<40} {header_pressao}  {'TOTAL':>10}")
    print(f"  {'-'*40} {'  '.join(['-'*10]*5)}  {'-'*10}")

    for t in tiers:
        if t not in tabela.index:
            continue
        row_vals = '  '.join(f"{int(tabela.loc[t, c]):>10,}" for c in cols_pressao)
        total_t  = int(tabela.loc[t, 'TOTAL'])
        pct_t    = total_t / total_geral * 100 if total_geral else 0
        print(f"  {label_tier(t):<40} {row_vals}  {total_t:>10,}  ({pct_t:.1f}%)")

    sep('-')
    total_vals = '  '.join(f"{int(tabela[c].sum()):>10,}" for c in cols_pressao)
    print(f"  {'TOTAL':<40} {total_vals}  {int(total_geral):>10,}")
    print()

    # Clientes com pressão máxima (>3) por tier
    alta_pressao = df[df['pressao_num'] > 3]
    if not alta_pressao.empty:
        print("  Clientes com pressão > 3 campanhas, por Tier:")
        for t in tiers:
            n = int((alta_pressao['tier'] == t).sum())
            if n > 0:
                pct = n / len(alta_pressao) * 100
                print(f"    {label_tier(t):<40} {n:>8,}  ({pct:.1f}%)")
        print()


# =============================================================================
# SECÇÃO 7 – ELEGIBILIDADE E SOBREPOSIÇÃO (SÍNTESE)
# =============================================================================
def caracterizar_elegibilidade(df_plano, df_base):
    titulo('7. ELEGIBILIDADE E COBERTURA DA BASE DE CLIENTES')

    # Colunas de elegibilidade: prefixo 'iXS_' identifica cada campanha na matriz de elegibilidade
    colunas_elegibilidade = [c for c in df_base.columns if c.startswith('iXS_')]
    if not colunas_elegibilidade:
        print("  ⚠  Sem colunas de elegibilidade (iXS_*) encontradas.\n")
        return

    total_clientes = len(df_base)

    # Clientes com pelo menos uma campanha elegível
    tem_elegivel  = (df_base[colunas_elegibilidade].sum(axis=1) > 0).sum()
    sem_elegivel  = total_clientes - tem_elegivel
    media_elegiveis = df_base[colunas_elegibilidade].sum(axis=1).mean()

    print(f"  Total de clientes                   : {total_clientes:>10,}")
    print(f"  Com pelo menos 1 campanha elegível  : {tem_elegivel:>10,}  ({tem_elegivel/total_clientes*100:.1f}%)")
    print(f"  Sem nenhuma campanha elegível       : {sem_elegivel:>10,}  ({sem_elegivel/total_clientes*100:.1f}%)")
    print(f"  Média de campanhas por cliente      : {media_elegiveis:>10.2f}")
    print()

    # Elegibilidade por campanha (apenas campanhas no plano do mês actual)
    camps_mes = df_plano[
        df_plano['data'].dt.month == hoje.month
    ]['campanha'].tolist()

    camps_com_dados = [c for c in camps_mes if c in df_base.columns]
    if camps_com_dados:
        print(f"  Elegibilidade por campanha (mês actual – {hoje.strftime('%Y-%m')}):")
        print(f"  {'Campanha':<35} {'Volumetria':>10}  {'Elegíveis':>10}  {'Cobertura':>10}  Escassez")
        print(f"  {'-'*35} {'-'*10}  {'-'*10}  {'-'*10}  {'-'*8}")
        for camp in camps_com_dados:
            vol  = int(df_plano.loc[df_plano['campanha'] == camp, 'volumetria'].iloc[0])
            eleg = int((df_base[camp] == 1).sum())
            cob  = eleg / total_clientes * 100 if total_clientes else 0
            esc  = vol  / max(eleg, 1)
            print(
                f"  {camp:<35} {vol:>10,}  {eleg:>10,}  {cob:>9.1f}%  {esc:.2f} {nivel_escassez(esc)}"
            )
        print()


# =============================================================================
# PONTO DE ENTRADA PRINCIPAL
# =============================================================================
def main():
    sep()
    print("  CARACTERIZAÇÃO DO PROJECTO DE CAMPANHAS")
    print(f"  Executado em: {hoje.strftime('%Y-%m-%d %H:%M:%S')}")
    sep()
    print()

    # --- Carregar plano ---
    try:
        df_plano = carregar_plano()
        print(f"  ✓ Plano carregado: {len(df_plano)} campanhas\n")
    except FileNotFoundError as e:
        print(f"  ✗ ERRO: {e}")
        return

    # --- Tentar carregar base de envio via SQL ---
    print("  A tentar ligar à base de dados SQL Server...")
    df_base = obter_base_envio()
    if df_base is not None:
        print(f"  ✓ Base de envio carregada: {len(df_base):,} clientes\n")
    else:
        print("  ℹ  A continuar apenas com dados do CSV.\n")

    sep()
    print()

    # --- Secções de caracterização ---
    caracterizar_plano(df_plano)

    camps_hoje = caracterizar_campanhas_hoje(df_plano, df_base)

    caracterizar_escassez(df_plano, df_base)

    if df_base is not None:
        df_com_tier = caracterizar_clientes_por_tier(df_base)
        caracterizar_clientes_por_pressao(df_base)
        caracterizar_cruzada_tier_pressao(df_base)
        caracterizar_elegibilidade(df_plano, df_base)
    else:
        print("=" * 80)
        print("  SECÇÕES 4–7 REQUEREM LIGAÇÃO SQL SERVER (#BaseEnvio)")
        print("  Execute o script a partir de uma máquina com acesso a Diomedes.")
        print("=" * 80)
        print()

    sep()
    print("  FIM DA CARACTERIZAÇÃO")
    sep()


if __name__ == '__main__':
    main()
