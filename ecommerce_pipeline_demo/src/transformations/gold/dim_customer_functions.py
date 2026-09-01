from pyspark.sql import functions as F


BRAZIL_STATES = {
    "AC": "Acre",
    "AL": "Alagoas",
    "AP": "Amapá",
    "AM": "Amazonas",
    "BA": "Bahia",
    "CE": "Ceará",
    "DF": "Distrito Federal",
    "ES": "Espírito Santo",
    "GO": "Goiás",
    "MA": "Maranhão",
    "MT": "Mato Grosso",
    "MS": "Mato Grosso do Sul",
    "MG": "Minas Gerais",
    "PA": "Pará",
    "PB": "Paraíba",
    "PR": "Paraná",
    "PE": "Pernambuco",
    "PI": "Piauí",
    "RJ": "Rio de Janeiro",
    "RN": "Rio Grande do Norte",
    "RS": "Rio Grande do Sul",
    "RO": "Rondônia",
    "RR": "Roraima",
    "SC": "Santa Catarina",
    "SP": "São Paulo",
    "SE": "Sergipe",
    "TO": "Tocantins",
}


def build_dim_customer(df):
    state_map = F.create_map(
        *[F.lit(x) for item in BRAZIL_STATES.items() for x in item]
    )

    return (
        df.filter(F.col("__END_AT").isNull())
        .withColumn("customer_key", F.xxhash64("customer_id"))
        .withColumn(
            "customer_state_name",
            F.element_at(state_map, F.col("customer_state")),
        )
        .withColumn(
            "customer_state_location",
            F.concat_ws(
                ", ",
                F.col("customer_state_name"),
                F.lit("Brazil"),
            ),
        )
        .select(
            "customer_key",
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
            "customer_state_name",
            "customer_state_location",
        )
    )