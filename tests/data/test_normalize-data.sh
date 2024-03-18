    #--------------------------------------------------------------------------
    # synopsis
    #--
    # DATA for tests normalize
    #   A4 address line 4
    #    A4N housenumber
    #    A4E extension
    #    A4S street
    #   A6 address line 6
    #    A6P postcode
    #    A6M municipality

declare -a TEST_A4S_NAME=(
    'RUE DU 19 FEVRIER 1416 CHEMIN DE LA DIGUE'                             #  1
    'RUE DU LIEUTENANT DE VAISSEAU D ESTIENNE D ORVES'                      #  2
    'RUE DU DOCTEUR PIERRE LAFLOTTE ET DE L ANCIEN HOPITAL'                 #  3
    'ZONE ARTISANALE ET COMMERCIALE DU HAUT DES TAPPES'                     #  4
    'CHEMIN DE NOTRE DAME DES CHAMPS ET DES VIGNES'                         #  5
    'PLACE NOTRE DAME DE LA LEGION D HONNEUR'                               #  6
    'PARC D ACTIVITES NURIEUX CROIX CHALON'                                 #  7
    'AVENUE DES ANCIENS COMBATTANTS FRANCAIS D INDOCHINE'                   #  8
    'CHEMIN RURAL 21 DES GRAVIERS DE LA CROIX DES MARAIS'                   #  9
    'ZONE ARTISANALE CENTRE COMMERCIAL BEAUGE'                              # 10
    'CHEMIN DE LA DEBOISSET CLOS SAINT ANNE'                                # 11
    'ANCIENNE ROUTE LE CHATEAU SAINT LEONARD'                               # 12
    'AVENUE DE LA 9E DIVISION INFANTERIE DE CAVALERIE'                      # 13
    'LIEU DIT LE GRAND BOIS DE LA DURANDIERE'                               # 14
    'CHEMIN D EXPLOITATION DU MAS SAINT PAUL'                               # 15
    'LIEU DIT LA MAISON FORESTIERE DE LA NOE BADON'                         # 16
    'LIEU DIT LA MAISON FORESTIERE DU VIRLET'                               # 17
    'CHEMIN RURAL 55 ANCIEN CHEMIN DE MONTPELLIER'                          # 18
    'CHEMIN RURAL DIT ANCIENNE ROUTE DE SEYSSEL A ANNECY'                   # 19
    'CHEMIN RURAL DIT ANCIEN CHEMIN DE BRISON A THUET'                      # 20
    'RUE DU TERTRE DE NOTRE DAME DE LA SALETTE'                             # 21
    'RUE DE L ADJUDANT BESNAULT ET DU GENDARME LEFORT'                      # 22
)

declare -a TEST_A4S_DESCRIPTORS=(
    VACNCTAAN                                                               #  1
    VATTTANAN                                                               #  2
    VATPNAAATN                                                              #  3
    VVANATAN                                                                #  4
    VATTANAAN                                                               #  5
    VTTAANAN                                                                #  6
    VANNTN                                                                  #  7
    VANNTAN                                                                 #  8
    VNCANAATAN                                                              #  9
    VVTTN                                                                   # 10
    VAANTTN                                                                 # 11
    VVATTN                                                                  # 12
    VAACTTAN                                                                # 13
    VVATTAAN                                                                # 14
    VANATTN                                                                 # 15
    VVATTAANN                                                               # 16
    VVATTAN                                                                 # 17
    VNCTTAN                                                                 # 18
    VNNTTANAN                                                               # 19
    VNNTTANAN                                                               # 20
    VATATTAAN                                                               # 21
    VAATNAANN                                                               # 22
)

declare -a TEST_A4S_NAME_NORMALIZED=(
    #         1         2         3
    #12345678901234567890123456789012
    'RUE 19 FEVRIER 1416 CHEMIN DIGUE'                                      #  1
    'RUE DU LTDV D ESTIENNE D ORVES'                                        #  2
    'R DR P LAFLOTTE L ANCIEN HOPITAL'                                      #  3
    'ZA ET COMMERCIALE DU HAUT TAPPES'                                      #  4
    'CHEM ND DES CHAMPS ET DES VIGNES'                                      #  5
    'PLACE ND DE LA LEGION D HONNEUR'                                       #  6
    'PARC A NURIEUX CRX CHALON'                                             #  7
    'AV ANC COMB FRANCAIS D INDOCHINE'                                      #  8
    'CHEMIN R 21 GRAVIERS CRX MARAIS'                                       #  9
    'ZA CENTRE COMMERCIAL BEAUGE'                                           # 10
    'CHEMIN DEBOISSET CLOS SAINT ANNE'                                      # 11
    'ANCI ROUTE CHATEAU SAINT LEONARD'                                      # 12
    'AV LA 9E DIV INFANT DE CAVALERIE'                                      # 13
    'LD GRAND BOIS DE LA DURANDIERE'                                        # 14
    'CHEM EXPLOITATION MAS SAINT PAUL'                                      # 15
    'LD MAIS FORESTIERE LA NOE BADON'                                       # 16
    'LD LA MAIS FORESTIERE DU VIRLET'                                       # 17
    'CHEMIN R 55 ANCI CHEMIN M'                                             # 18
    'CHEMIN R D ANCI ROUTE S ANNECY'                                        # 19
    'CHEMIN R D ANCI CHEMIN B THUET'                                        # 20
    'RUE TRT NOTRE DAME DE LA SALETTE'                                      # 21
    'R L ADJ BESNAULT GENDARME LEFORT'                                      # 22
)

declare -a TEST_A4S_DESCRIPTORS_NORMALIZED=(
    VCNCTN                                                                  #  1
    VATANAN                                                                 #  2
    VTPNATN                                                                 #  3
    VANATN                                                                  #  4
    VTANAAN                                                                 #  5
    VTAANAN                                                                 #  6
    VNNTN                                                                   #  7
    VNNTAN                                                                  #  8
    VNCNTN                                                                  #  9
    VTTN                                                                    # 10
    VNTTN                                                                   # 11
    VVTTN                                                                   # 12
    VACTTAN                                                                 # 13
    VTTAAN                                                                  # 14
    VNTTN                                                                   # 15
    VTTANN                                                                  # 16
    VATTAN                                                                  # 17
    VNCTTN                                                                  # 18
    VNNTTNN                                                                 # 19
    VNNTTNN                                                                 # 20
    VTTTAAN                                                                 # 21
    VATNNN                                                                  # 22
)

declare -a TEST_A4S_SPLIT_NAME_NORMALIZED=(
    '{RUE,19,FEVRIER,1416,CHEMIN,DIGUE}'                                    #  1
    '{RUE,DU,LTDV,D,ESTIENNE,D,ORVES}'                                      #  2
    '{R,DR,P,LAFLOTTE,L,ANCIEN,HOPITAL}'                                    #  3
    '{ZA,ET,COMMERCIALE,DU,HAUT,TAPPES}'                                    #  4
    '{CHE,"NOTRE DAME",CHAMPS,DES,VIGNES}'                                  #  5
    '{PLACE,ND,"DE,LA",LEGION,D,HONNEUR}'                                   #  6
    '{PARC,"A NURIEUX",CRX,CHALON}'                                         #  7
    '{AV,"ANC COMB",FRANCAIS,D,INDOCHINE}'                                  #  8
    '{CHEM,R,21,GRAVIERS,CRX,MARAIS}'                                       #  9
    '{ZA,"CENTRE COMMERCIAL",BEAUGE}'                                       # 10
    '{CHE,LA,DEBOISSET,"CLOS SAINT",ANNE}'                                  # 11
    '{"ANCI ROUTE","CHATEAU SAINT",LEONARD}'                                # 12
    '{AV,LA,9E,"DIV INFANT",DE,CAVALERIE}'                                  # 13
    '{LD,"GRAND BOIS","DE LA",DURANDIERE}'                                  # 14
    '{CHE,EXPLOITATION,"MAS ST",PAUL}'                                      # 15
    '{LD,"MAIS FORESTIERE",LA,"NOE BADON"}'                                 # 16
    '{LD,LA,"MAIS FORESTIERE",DU,VIRLET}'                                   # 17
    '{CHE,R,55,"ANCI CHEMIN",MONTPELLIER}'                                  # 18
    '{CHEMIN,"R D","ANCI ROUTE","S ANNECY"}'                                # 19
    '{CHE,"R D","ANCI CHEMIN","BRISON THUET"}'                              # 20
    '{RUE,"TRT NOTRE DAME","DE LA",SALETTE}'                                # 21
    '{R,L,ADJ,"BESNAULT GENDARME LEFORT"}'                                  # 22
)

declare -a TEST_A4S_SPLIT_DESCRIPTORS_NORMALIZED=(
    '{V,C,N,C,T,N}'                                                         #  1
    '{V,A,T,A,N,A,N}'                                                       #  2
    '{V,T,P,N,A,T,N}'                                                       #  3
    '{V,A,N,A,T,N}'                                                         #  4
    '{V,TT,N,A,N}'                                                          #  5
    '{V,T,AA,N,A,N}'                                                        #  6
    '{V,NN,T,N}'                                                            #  7
    '{V,NN,T,A,N}'                                                          #  8
    '{V,N,C,N,T,N}'                                                         #  9
    '{V,TT,N}'                                                              # 10
    '{V,A,N,TT,N}'                                                          # 11
    '{VV,TT,N}'                                                             # 12
    '{V,A,C,TT,A,N}'                                                        # 13
    '{V,TT,AA,N}'                                                           # 14
    '{V,N,TT,N}'                                                            # 15
    '{V,TT,A,NN}'                                                           # 16
    '{V,A,TT,A,N}'                                                          # 17
    '{V,N,C,TT,N}'                                                          # 18
    '{V,NN,TT,NN}'                                                          # 19
    '{V,NN,TT,NN}'                                                          # 20
    '{V,TTT,AA,N}'                                                          # 21
    '{V,A,T,NNN}'                                                           # 22
)

declare -i TEST_A4S_SZ=${#TEST_A4S_NAME[*]}
