{
  "reports_request": [
    {
      "environment": "PROD",
      "report_name": "chedraui",
      "schema": "mx",
      "report_to": [
        "cvargas@georesearch.cl"
      ],
      "drop_workflow": false,
      "etl_list": [
        "local"
      ],
      "parametros": {
        "delivery": {
          "cannibalization_shape": "POLYGON((-70.5943113357452 -33.47630754684649,-70.5943352310499 -33.4764831778805,-70.5943995539812 -33.4766515275262,-70.5945018328414 -33.47680612613581,-70.5946381372352 -33.4769410324808,-70.5948032290932 -33.4770510620836,-70.5949907639637 -33.47713198647052,-70.5951935348359 -33.477180695683295,-70.5954037491242 -33.4771953178066,-70.5956133281658 -33.47717529091041,-70.5958142177162 -33.4771213846475,-70.5959986975062 -33.4770356706707,-70.5961596779572 -33.4769214430107,-70.5962909726485 -33.476783091474694,-70.596387536063 -33.4766259329326,-70.5964456574739 -33.4764560069781,-70.5964631035224 -33.4762798438185,-70.5964392040125 -33.476104213314706,-70.5963748776282 -33.4759358648184,-70.5962725965929 -33.4757812678024,-70.5961362916331 -33.4756463632528,-70.5959712009042 -33.4755363353734,-70.5957836686862 -33.475455412376,-70.5955809015859 -33.4754067040069,-70.5953706916148 -33.47539208205341,-70.5951611167784 -33.47541210841929,-70.5949602306809 -33.475466013532795,-70.594775753066 -33.47555172591589,-70.594614773181 -33.4756659517807,-70.5944834773606 -33.4758043015932,-70.5943869112937 -33.4759614587459,-70.5943287861109 -33.4761313838565,-70.5943113357452 -33.47630754684649))",
          "canasta_categoria_id": [
            34
          ],
          "substring_id": [
            4,
            5,
            6
          ],
          "pois_category_id": [
            10008
          ]
        },
        "local": {
          "buffer_search": 2000,
          "pois_state_id": 1,
          "surface_factor": 1.1,
          "distance_factor": -1.95, 
          "canasta_categoria_id": [
            34 ,35 ,37
          ],
          "substring_id": [ ],
          "pois_category_id": [
            10008
          ]
        }
      }
    }
  ]
}