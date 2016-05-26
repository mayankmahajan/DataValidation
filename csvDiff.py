# from
if __name__ == '__main__':

    parquetCSVs = '/Users/mayank.mahajan/PycharmProjects/DataValidation/nfnameid_sourcesiteid=18/'
    uiCSVs = '/Users/mayank.mahajan/Downloads'
    peakUplink1 = 'PEAK_1455015680.0871618124400.csv'
    peakUplink2 = ''

    # csv1 = 'destsiteid_destsitetypeid=1;sourcesitetypeid=1;sourcesiteid=18/PEAK_uplinkbitbuffer.csv'
    # csv2 = 'Results_2016-01-14T05-44-38.csv'



    diff_str  = utils.csv_diff(csv1,csv2)
    print diff_str