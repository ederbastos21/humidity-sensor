# -*- coding: utf-8 -*-
"""
CSV -> Excel para análise/calibração de 3 sensores.
Instale: pip install pandas openpyxl
Execute: python exportar_calibracao.py
"""
import csv, re
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, simpledialog, messagebox
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.chart import LineChart, Reference
from openpyxl.formatting.rule import ColorScaleRule
from openpyxl.utils import get_column_letter

def read_csv(path):
    for enc in ("utf-8-sig", "utf-8", "latin1"):
        try:
            with open(path, encoding=enc, newline="") as f: sample=f.read(10000)
            try: sep=csv.Sniffer().sniff(sample, delimiters=";,\\t,").delimiter
            except csv.Error: sep=";"
            df=pd.read_csv(path, sep=sep, encoding=enc, engine="python")
            # Converte números com vírgula decimal quando possível
            for c in df.columns:
                if df[c].dtype == object:
                    s=df[c].astype(str).str.strip()
                    v=pd.to_numeric(s.str.replace(",", ".", regex=False), errors="coerce")
                    if v.notna().sum() >= max(1, .8*s.notna().sum()): df[c]=v
            return df
        except UnicodeDecodeError: pass
    raise ValueError("Não foi possível ler o CSV/encoding.")

def detect(cols):
    out={}
    for c in cols:
        name=str(c).lower()
        for n in (1,2,3):
            patterns=[rf"sensor\\s*[_-]?{n}(?!\\d)",rf"sens\\s*[_-]?{n}(?!\\d)",
                      rf"cap(?:acitivo)?\\s*[_-]?{n}(?!\\d)",rf"ch\\s*[_-]?{n}(?!\\d)"]
            if any(re.search(p,name) for p in patterns): out[n]=c; break
    return out if len(out)==3 else None

def choose_cols(df, root):
    nums=[c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    if len(nums)<3: raise ValueError("CSV precisa conter pelo menos 3 colunas numéricas.")
    auto=detect(df.columns)
    if auto and messagebox.askyesno("Colunas",f"Detectei:\\nSensor 1: {auto[1]}\\nSensor 2: {auto[2]}\\nSensor 3: {auto[3]}\\n\\nUsar?"):
        return auto[1],auto[2],auto[3]
    chosen=[]
    listing="\\n".join(f"{i+1}. {c}" for i,c in enumerate(nums))
    for n in (1,2,3):
        x=simpledialog.askinteger("Coluna do sensor",f"Digite o número da coluna do SENSOR {n}:\\n\\n{listing}",minvalue=1,maxvalue=len(nums))
        if x is None: raise RuntimeError("Cancelado.")
        chosen.append(nums[x-1])
    if len(set(chosen))<3: raise ValueError("Escolha três colunas diferentes.")
    return tuple(chosen)

def make_excel(df, sensors, step, source, output):
    s1,s2,s3=sensors
    full=df.copy()
    full.insert(0,"Amostra_original",range(1,len(df)+1))
    sampled=full.iloc[::step].copy().reset_index(drop=True)
    # Colunas auxiliares; calibração é editada na aba Calibracao
    sampled["S1_referencia"]=pd.to_numeric(sampled[s1],errors="coerce")
    sampled["S2_offset"]=0; sampled["S2_fator"]=1
    sampled["S2_corrigido"]=pd.to_numeric(sampled[s2],errors="coerce")
    sampled["S3_offset"]=0; sampled["S3_fator"]=1
    sampled["S3_corrigido"]=pd.to_numeric(sampled[s3],errors="coerce")
    sampled["Erro_S2"]=sampled["S2_corrigido"]-sampled["S1_referencia"]
    sampled["Erro_S3"]=sampled["S3_corrigido"]-sampled["S1_referencia"]
    cal=pd.DataFrame([
      ["Sensor de referência","Sensor 1","Referência de comparação (não necessariamente mais preciso)"],
      ["Coluna CSV Sensor 1",str(s1),"Identificada/selecionada"],
      ["Coluna CSV Sensor 2",str(s2),"Identificada/selecionada"],
      ["Coluna CSV Sensor 3",str(s3),"Identificada/selecionada"],
      ["Offset Sensor 2",0,"EDITÁVEL: somado à leitura RAW antes do fator"],
      ["Fator Sensor 2",1,"EDITÁVEL: multiplicador"],
      ["Offset Sensor 3",0,"EDITÁVEL: somado à leitura RAW antes do fator"],
      ["Fator Sensor 3",1,"EDITÁVEL: multiplicador"],
      ["Intervalo (linhas)",step,"Uma amostra exportada a cada N linhas"],
      ["Total de amostras CSV",len(df),""],
      ["Amostras exportadas",len(sampled),""],
      ["MAE Sensor 2",None,"Erro absoluto médio"],
      ["RMSE Sensor 2",None,"Raiz do erro quadrático médio"],
      ["Erro médio Sensor 2",None,"Média (com sinal)"],
      ["MAE Sensor 3",None,"Erro absoluto médio"],
      ["RMSE Sensor 3",None,"Raiz do erro quadrático médio"],
      ["Erro médio Sensor 3",None,"Média (com sinal)"],
    ],columns=["Parâmetro","Valor","Descrição"])
    guide=pd.DataFrame({"INSTRUÇÕES":[
      "1. Edite offsets/fatores amarelos na aba Calibracao.",
      "2. Fórmula: corrigido = (RAW + offset) * fator.",
      "3. Sensor 1 é referência e não recebe correção.",
      "4. Dados_amostrados calcula sensores corrigidos e erros contra S1.",
      "5. Dados_completos mantém todas as linhas; Dados_amostrados reduz conforme intervalo.",
      "6. Use condições comparáveis (mesmo material/compactação) para calibrar.",
      "7. Esta planilha alinha sensores; não converte RAW em umidade real sem referência gravimétrica.",
      f"CSV de origem: {Path(source).name}",f"Colunas: S1={s1}; S2={s2}; S3={s3}"
    ]})
    with pd.ExcelWriter(output,engine="openpyxl") as w:
        sampled.to_excel(w,sheet_name="Dados_amostrados",index=False)
        full.to_excel(w,sheet_name="Dados_completos",index=False)
        cal.to_excel(w,sheet_name="Calibracao",index=False)
        guide.to_excel(w,sheet_name="Instrucoes",index=False)

    wb=load_workbook(output); ws=wb["Dados_amostrados"]; wc=wb["Calibracao"]
    headers={ws.cell(1,c).value:c for c in range(1,ws.max_column+1)}
    n=ws.max_row
    # Índices de colunas
    raw2,raw3=headers[s2],headers[s3]; ref=headers["S1_referencia"]
    off2,fac2,cor2=headers["S2_offset"],headers["S2_fator"],headers["S2_corrigido"]
    off3,fac3,cor3=headers["S3_offset"],headers["S3_fator"],headers["S3_corrigido"]
    e2,e3=headers["Erro_S2"],headers["Erro_S3"]
    for r in range(2,n+1):
        ws.cell(r,off2,"='Calibracao'!$B$6"); ws.cell(r,fac2,"='Calibracao'!$B$7")
        ws.cell(r,cor2,f"=({get_column_letter(raw2)}{r}+{get_column_letter(off2)}{r})*{get_column_letter(fac2)}{r}")
        ws.cell(r,off3,"='Calibracao'!$B$8"); ws.cell(r,fac3,"='Calibracao'!$B$9")
        ws.cell(r,cor3,f"=({get_column_letter(raw3)}{r}+{get_column_letter(off3)}{r})*{get_column_letter(fac3)}{r}")
        ws.cell(r,e2,f"={get_column_letter(cor2)}{r}-{get_column_letter(ref)}{r}")
        ws.cell(r,e3,f"={get_column_letter(cor3)}{r}-{get_column_letter(ref)}{r}")
    L=lambda c:get_column_letter(c)
    a=f"'Dados_amostrados'!{L(e2)}2:{L(e2)}{n}"; b=f"'Dados_amostrados'!{L(e3)}2:{L(e3)}{n}"
    wc["B12"]=f"=IFERROR(SUMPRODUCT(ABS({a}))/COUNT({a}),0)"
    wc["B13"]=f"=IFERROR(SQRT(SUMSQ({a})/COUNT({a})),0)"
    wc["B14"]=f"=IFERROR(AVERAGE({a}),0)"
    wc["B15"]=f"=IFERROR(SUMPRODUCT(ABS({b}))/COUNT({b}),0)"
    wc["B16"]=f"=IFERROR(SQRT(SUMSQ({b})/COUNT({b})),0)"
    wc["B17"]=f"=IFERROR(AVERAGE({b}),0)"
    blue="1F4E78"; yellow="FFF2CC"; green="E2F0D9"; thin=Side(style="thin",color="D9E1F2")
    for sh in wb.worksheets:
        sh.freeze_panes="A2"; sh.auto_filter.ref=sh.dimensions; sh.row_dimensions[1].height=30
        for cell in sh[1]:
            cell.fill=PatternFill("solid",fgColor=blue); cell.font=Font(color="FFFFFF",bold=True)
            cell.alignment=Alignment(horizontal="center",vertical="center",wrap_text=True)
        for col in sh.columns:
            letter=get_column_letter(col[0].column)
            width=max((len(str(x.value)) if x.value is not None else 0 for x in col[:150]),default=10)
            sh.column_dimensions[letter].width=min(max(width+2,12),36)
        for row in sh.iter_rows():
            for cell in row: cell.border=Border(bottom=thin)
    for c in ("B6","B7","B8","B9"):
        wc[c].fill=PatternFill("solid",fgColor=yellow); wc[c].font=Font(bold=True,color="9C6500")
    for c in ("B12","B13","B14","B15","B16","B17"):
        wc[c].fill=PatternFill("solid",fgColor=green); wc[c].number_format="0.00"
    for c in (e2,e3):
        ws.conditional_formatting.add(f"{L(c)}2:{L(c)}{n}",ColorScaleRule(start_type="min",start_color="F8696B",mid_type="percentile",mid_value=50,mid_color="FFEB84",end_type="max",end_color="63BE7B"))
    chart=LineChart(); chart.title="Leitura: referência e sensores corrigidos"; chart.y_axis.title="RAW"; chart.x_axis.title="Amostra"
    chart.height=9; chart.width=20
    for c in (ref,cor2,cor3): chart.add_data(Reference(ws,min_col=c,min_row=1,max_row=min(n,1001)),titles_from_data=True)
    chart.set_categories(Reference(ws,min_col=headers["Amostra_original"],min_row=2,max_row=min(n,1001)))
    ws.add_chart(chart,f"A{n+3}")
    wb.calculation.fullCalcOnLoad=True; wb.calculation.forceFullCalc=True; wb.calculation.calcMode="auto"
    wb.save(output)

def main():
    root=tk.Tk(); root.withdraw()
    csvpath=filedialog.askopenfilename(title="Escolha o CSV",filetypes=[("CSV","*.csv"),("Todos","*.*")])
    if not csvpath:return
    step=simpledialog.askinteger("Amostragem","Registrar uma linha a cada quantas amostras?\\nEx.: 10 = uma linha a cada 10.",initialvalue=10,minvalue=1)
    if step is None:return
    try:
        df=read_csv(csvpath); sensors=choose_cols(df,root)
        out=filedialog.asksaveasfilename(title="Salvar Excel",defaultextension=".xlsx",initialfile=Path(csvpath).stem+"_calibracao.xlsx",filetypes=[("Excel","*.xlsx")])
        if not out:return
        make_excel(df,sensors,step,csvpath,out)
        messagebox.showinfo("Pronto",f"Excel criado:\\n{out}\\n\\nLinhas CSV: {len(df)}\\nLinhas exportadas: {len(df.iloc[::step])}")
    except Exception as e: messagebox.showerror("Erro",str(e)); raise
if __name__=="__main__": main()
