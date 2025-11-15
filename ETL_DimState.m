// ETL_DimState.m
// Propósito: Tabla auxiliar de mapeo de siglas de estado de Brasil a nombre completo.
// Utilizado en DimCustomer y DimSeller.

shared DimState = let
  Source = #table(
    type table [state_abbreviation = text, state_full_name = text],
    {
      {"SP", "Sao Paulo"},
      {"SC", "Santa Catarina"},
      {"MG", "Minas Gerais"},
      {"RJ", "Rio de Janeiro"},
      {"RS", "Rio Grande do Sul"},
      {"PA", "Pará"},
      {"GO", "Goiás"},
      {"MA", "Maranhao"},
      {"PR", "Paraná"}, 
      {"BA", "Bahía"},
      {"ES", "Espirito Santo"},
      {"MS", "Mato Grosso do Sul"},
      {"CE", "Ceará"},
      {"DF", "Distrito Federal"},
      {"RN", "Rio Grande do Norte"},
      {"AL", "Alagoas"},
      {"PE", "Pernambuco"},
      {"AM", "Amazonas"},
      {"MT", "Mato Grosso"},
      {"TO", "Tocantins"},
      {"RO", "Rondonia"},
      {"SE", "Sergipe"},
      {"AP", "Amapá"},
      {"PB", "Paraíba"},
      {"PI", "Piauí"},
      {"AC", "Acre"}
    }
  ),
  // Se renombra para uso específico en DimSeller/DimCustomer
  Rename = Table.RenameColumns(Source, {{"state_abbreviation", "state_abbr"}, {"state_full_name", "state_name"}})
in
  Rename
