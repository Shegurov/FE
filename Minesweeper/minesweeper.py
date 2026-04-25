import random
import os
import sys


# ANSI colors for mine numbers
COLORS = {
    1: '\033[94m',   # blue
    2: '\033[92m',   # green
    3: '\033[91m',   # red
    4: '\033[34m',   # dark blue
    5: '\033[31m',   # dark red
    6: '\033[96m',   # cyan
    7: '\033[35m',   # magenta
    8: '\033[90m',   # grey
}
RESET = '\033[0m'
BOLD  = '\033[1m'


def clear():
    os.system('cls' if os.name == 'nt' else 'clear')


class Board:
    def __init__(self, rows, cols, mines):
        self.rows  = rows
        self.cols  = cols
        self.total_mines = mines

        self._grid     = [[0]     * cols for _ in range(rows)]  # -1 = mine, 0-8 = count
        self.revealed  = [[False] * cols for _ in range(rows)]
        self.flagged   = [[False] * cols for _ in range(rows)]

        self.game_over  = False
        self.won        = False
        self._first     = True          # mines placed on first reveal

    # ------------------------------------------------------------------ setup

    def _place_mines(self, safe_r, safe_c):
        """Place mines after the first move, keeping a 3×3 safe zone."""
        safe = {
            (safe_r + dr, safe_c + dc)
            for dr in range(-1, 2)
            for dc in range(-1, 2)
            if 0 <= safe_r + dr < self.rows and 0 <= safe_c + dc < self.cols
        }
        candidates = [
            (r, c)
            for r in range(self.rows)
            for c in range(self.cols)
            if (r, c) not in safe
        ]
        count = min(self.total_mines, len(candidates))
        for r, c in random.sample(candidates, count):
            self._grid[r][c] = -1

        for r in range(self.rows):
            for c in range(self.cols):
                if self._grid[r][c] == -1:
                    continue
                self._grid[r][c] = sum(
                    1
                    for dr in range(-1, 2)
                    for dc in range(-1, 2)
                    if (dr or dc)
                    and 0 <= r + dr < self.rows
                    and 0 <= c + dc < self.cols
                    and self._grid[r + dr][c + dc] == -1
                )

    # ----------------------------------------------------------------- actions

    def reveal(self, r, c):
        if not (0 <= r < self.rows and 0 <= c < self.cols):
            return
        if self.revealed[r][c] or self.flagged[r][c] or self.game_over:
            return

        if self._first:
            self._place_mines(r, c)
            self._first = False

        self.revealed[r][c] = True

        if self._grid[r][c] == -1:
            self.game_over = True
            return

        # flood-fill for empty cells
        if self._grid[r][c] == 0:
            for dr in range(-1, 2):
                for dc in range(-1, 2):
                    if dr or dc:
                        self.reveal(r + dr, c + dc)

        self._check_win()

    def toggle_flag(self, r, c):
        if not (0 <= r < self.rows and 0 <= c < self.cols):
            return
        if self.revealed[r][c] or self.game_over:
            return
        self.flagged[r][c] = not self.flagged[r][c]

    # ----------------------------------------------------------------- helpers

    def _check_win(self):
        for r in range(self.rows):
            for c in range(self.cols):
                if self._grid[r][c] != -1 and not self.revealed[r][c]:
                    return
        self.won = self.game_over = True

    @property
    def flags_used(self):
        return sum(self.flagged[r][c] for r in range(self.rows) for c in range(self.cols))

    # ----------------------------------------------------------------- display

    def render(self):
        clear()
        remaining = self.total_mines - self.flags_used
        print(f"\n  {BOLD}САПЁР{RESET}   |   Мин осталось: {BOLD}{remaining}{RESET}\n")

        # column header
        header = "      " + "".join(f"{c:3}" for c in range(self.cols))
        print(header)
        print("     " + "───" * self.cols)

        for r in range(self.rows):
            row = f"  {r:2} │"
            for c in range(self.cols):
                row += " " + self._cell_str(r, c)
            print(row)

        print()

    def _cell_str(self, r, c):
        if self.flagged[r][c] and not self.revealed[r][c]:
            return "F"
        if not self.revealed[r][c]:
            # show mine positions after game over loss
            if self.game_over and not self.won and self._grid[r][c] == -1:
                return "*"
            return "■"
        val = self._grid[r][c]
        if val == -1:
            return "X"            # the mine the player hit
        if val == 0:
            return "."
        return f"{COLORS.get(val, '')}{val}{RESET}"


# -------------------------------------------------------------------- UI helpers

def choose_difficulty():
    presets = {
        '1': (9,  9,  10, "Лёгкая    (9×9,  10 мин)"),
        '2': (16, 16, 40, "Средняя   (16×16, 40 мин)"),
        '3': (16, 30, 99, "Сложная   (16×30, 99 мин)"),
        '4': (0,  0,  0,  "Своя..."),
    }
    clear()
    print(f"\n  {BOLD}=== САПЁР ==={RESET}\n")
    for key, (_, _, _, label) in presets.items():
        print(f"  {key}. {label}")
    print()

    while True:
        choice = input("  Выбор [1-4]: ").strip()
        if choice in ('1', '2', '3'):
            r, c, m, _ = presets[choice]
            return r, c, m
        if choice == '4':
            try:
                rows  = max(5,  min(24, int(input("  Строки   (5-24): "))))
                cols  = max(5,  min(40, int(input("  Столбцы  (5-40): "))))
                limit = rows * cols - 9
                mines = max(1,  min(limit, int(input(f"  Мины     (1-{limit}): "))))
                return rows, cols, mines
            except (ValueError, KeyboardInterrupt):
                print("  Неверный ввод, попробуйте ещё раз.")


def parse_input(text):
    """Return ('r'|'f', row, col), ('q',), or None on bad input."""
    parts = text.strip().lower().split()
    if not parts:
        return None
    if parts[0] == 'q':
        return ('q',)
    if len(parts) == 3 and parts[0] in ('r', 'f'):
        try:
            return (parts[0], int(parts[1]), int(parts[2]))
        except ValueError:
            pass
    return None


def print_help(board):
    print(f"  r <строка> <столбец>  — открыть клетку   (0-{board.rows - 1}, 0-{board.cols - 1})")
    print(f"  f <строка> <столбец>  — поставить/снять флаг")
    print(f"  q                     — выйти\n")


# ---------------------------------------------------------------------- main

def main():
    while True:
        rows, cols, mines = choose_difficulty()
        board = Board(rows, cols, mines)
        board.render()
        print_help(board)

        while not board.game_over:
            try:
                raw = input("  Ввод: ")
            except (EOFError, KeyboardInterrupt):
                print("\n  До свидания!\n")
                sys.exit(0)

            cmd = parse_input(raw)

            if cmd is None:
                board.render()
                print("  Неверная команда. Примеры:  r 3 4   f 0 0   q\n")
                continue

            if cmd[0] == 'q':
                print("\n  До свидания!\n")
                sys.exit(0)

            action, r, c = cmd
            if not (0 <= r < board.rows and 0 <= c < board.cols):
                board.render()
                print(f"  Координаты вне поля. Строки: 0-{board.rows-1}, столбцы: 0-{board.cols-1}\n")
                continue

            if action == 'r':
                board.reveal(r, c)
            else:
                board.toggle_flag(r, c)

            board.render()
            if board.game_over:
                if board.won:
                    print(f"  {BOLD}Поздравляем! Вы нашли все мины!{RESET}\n")
                else:
                    print(f"  {BOLD}БУМ! Вы подорвались на мине.{RESET}\n")

        try:
            again = input("  Сыграть ещё раз? [д/н]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            again = 'н'

        if again not in ('д', 'да', 'y', 'yes'):
            print("\n  До свидания!\n")
            break


if __name__ == '__main__':
    main()
