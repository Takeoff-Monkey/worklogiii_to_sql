from utils import connect_postgres, setup_agent, get_result, pprint_sql


def main():
    engine = connect_postgres()
    if not engine:
        return

    setup = setup_agent(engine)
    if not setup:
        return
    agent_executor, query_logger = setup
    print("\nsql agent is ready ask your questions\n")
    print("type 'exit' or 'quit' to end session")

    while True:
        try:
            user_query = input("\n>>>")
            if user_query.lower() in ["exit", "quit"]:
                print("fine then fuck off")
                break

            if not user_query.strip():
                continue

            answer, sql_query = get_result(user_query, agent_executor, query_logger)

            print("\n ---- generated SQL ---- ")
            print(sql_query)
            print("\n ---- answer ----")
            print(answer)

        except Exception as e:
            print(f"\n an error occurred: {e}")
        except KeyboardInterrupt:
            print("\n session ended by user")
            break


if __name__ == "__main__":
    main()
