if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here

    print(f"Preprocessing: rows with zero passengers: { data['passenger_count'].isin([0]).sum()}")
    data = data[data['passenger_count'] > 0]

    print(f"Preprocessing: rows with zero trip_distance: { data['trip_distance'].isin([0]).sum()}")
    data = data[data['trip_distance'] > 0]

    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    data.columns = (data.columns
                .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                .str.lower()
             )
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['passenger_count'].isin([0]).sum()==0 , 'There are rides with zero passengers'
    print("No rides with zero passengers")

    assert output['trip_distance'].isin([0]).sum()==0 , 'There are rides with zero trip_distance'
    print("No trips with zero disance.")

    assert 'vendor_id' in output.columns, 'data.columns  is not one of the database columns'
    print("vendr_id is one of data base columns.")


