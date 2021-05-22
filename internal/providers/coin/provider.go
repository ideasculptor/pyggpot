package coin_provider

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"

	"github.com/aspiration-labs/pyggpot/internal/models"
	coin_service "github.com/aspiration-labs/pyggpot/rpc/go/coin"
	"github.com/twitchtv/twirp"
)

type coinServer struct {
	DB *sql.DB
}

func New(db *sql.DB) *coinServer {
	return &coinServer{
		DB: db,
	}
}

func (s *coinServer) AddCoins(ctx context.Context, request *coin_service.AddCoinsRequest) (*coin_service.CoinsListResponse, error) {
	if err := request.Validate(); err != nil {
		return nil, twirp.InvalidArgumentError(err.Error(), "")
	}

	tx, err := s.DB.Begin()
	if err != nil {
		return nil, twirp.InternalError(err.Error())
	}
	for _, coin := range request.Coins {
		fmt.Println(coin)
		newCoin := models.Coin{
			PotID:        request.PotId,
			Denomination: int32(coin.Kind),
			CoinCount:    coin.Count,
		}
		err = newCoin.Save(tx)
		if err != nil {
			return nil, twirp.InvalidArgumentError(err.Error(), "")
		}
	}
	err = tx.Commit()
	if err != nil {
		return nil, twirp.NotFoundError(err.Error())
	}

	return &coin_service.CoinsListResponse{
		Coins: request.Coins,
	}, nil
}

func (s *coinServer) RemoveCoins(ctx context.Context, request *coin_service.RemoveCoinsRequest) (*coin_service.CoinsListResponse, error) {
	if err := request.Validate(); err != nil {
		return nil, twirp.InvalidArgumentError(err.Error(), "")
	}

	tx, err := s.DB.Begin()
	if err != nil {
		return nil, twirp.InternalError(err.Error())
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	coinsInPot, err := models.CoinsInPotsByPot_id(tx, int(request.PotId))
	if err != nil {
		return nil, twirp.InternalError(err.Error())
	}

	coinsRemoved := s.shakePot(coinsInPot, request.Count)

	// now iterate over coinsInPot, deleting any with count = 0
	// and updating the others
	for _, coinFromPot := range coinsInPot {
		coin, err := models.CoinByID(tx, coinFromPot.ID)
		if err != nil {
			return nil, twirp.InternalError(err.Error())
		}
		coin.CoinCount = coinFromPot.CoinCount
		if coin.CoinCount == 0 {
			err = coin.Delete(tx)
		} else {
			err = coin.Save(tx)
		}
		if err != nil {
			return nil, twirp.InternalError(err.Error())
		}
	}

	err = tx.Commit()
	if err != nil {
		return nil, twirp.InternalError(err.Error())
	}
	committed = true

	return &coin_service.CoinsListResponse{
		Coins: coinsRemoved,
	}, nil
}

// shakePot jumps through a lot of hoops to ensure that it handles
// multiple CoinsInPot with the same denomination, potentially with
// count = 0.  It also relies on modifying the counts in the pot arg
// as a side-effect, which I wouldn't ordinarily do or allow, but
// there are limits to how much time I'm going to devote to this.
// same goes for refactoring this into shorter functions
//
// Basic algorithm:
//
// compute count of each denomination of coins in pot and total count
// generate random number between [0, total)
// If number is between [0, gold_count), remove a gold coin.
// If number is between [gold_count, gold_count + silver_count), remove silver coin
// If number is >= gold_count + silver_count, remove bronze coin
// so long as random number generator has even distribution, we will
// end up removing coins proportionally to their count in the pot.
//
// Relies on side effect of modifying instances pointed to by pot in
// order to communicate both the set of coins removed and the new state
// of coins in pot, which is a code smell, but quick to implement
func (s *coinServer) shakePot(pot []*models.CoinsInPot, count int32) []*coin_service.Coins {

	// map of denomination to count
	coinCounts := make(map[int32]int32, len(pot))
	// map of denomination to array of models.CoinsInPot
	coins := make(map[int32][]*models.CoinsInPot, len(pot))
	// total coins in the pot
	totalCoins := int32(0)
	// populate our maps and compute totalCoins
	for _, coin := range pot {
		if coin == nil {
			continue
		}
		coinCounts[coin.Denomination] += coin.CoinCount
		coins[coin.Denomination] = append(coins[coin.Denomination], coin)
		totalCoins += coin.CoinCount
	}

	results := make(map[int32]*coin_service.Coins, 3)
	// we know the total number of coins in the pot and the number
	// of each denomination. Now iterate, 'removing' coins until
	// empty or count coins have been removed
	for i := int32(0); i < count && totalCoins > 0; i++ {
		// random int from [0, totalCoins)
		idx := rand.Int31n(totalCoins)
		// map idx to a kind
		kind := int32(coin_service.Coins_UNKNOWN)
		switch {
		case idx < coinCounts[int32(coin_service.Coins_GOLD)]:
			kind = int32(coin_service.Coins_GOLD)
		case idx >= coinCounts[int32(coin_service.Coins_GOLD)] && idx < coinCounts[int32(coin_service.Coins_GOLD)]+coinCounts[int32(coin_service.Coins_SILVER)]:
			kind = int32(coin_service.Coins_SILVER)
		case idx >= coinCounts[int32(coin_service.Coins_GOLD)]+coinCounts[int32(coin_service.Coins_SILVER)]:
			kind = int32(coin_service.Coins_BRONZE)
		}

		// now remove a coin of the specified kind from our data structures
		totalCoins -= 1
		coinCounts[kind] -= 1
		// find non-empty coin to decrement from
		for _, coin := range coins[kind] {
			if coin.CoinCount > 0 {
				coin.CoinCount -= 1
				break
			}
		}
		// and add a coin to the results map
		removed, ok := results[kind]
		if !ok {
			results[kind] = &coin_service.Coins{
				Kind:  coin_service.Coins_Kind(kind),
				Count: 1,
			}
		} else {
			removed.Count++
		}
	}
	// convert map of Coins to slice of Coins
	removedCoins := make([]*coin_service.Coins, 0, len(results))
	for _, v := range results {
		removedCoins = append(removedCoins, v)
	}
	return removedCoins
}

// ListCoins added to enable validation that RemoveCoins works correctly
// without having to mock out a DB connection and otherwise build test
// infrastructure. It was much faster to just cut and paste this together
// to see what current state of a pot is.
func (s *coinServer) ListCoins(ctx context.Context, request *coin_service.ListCoinsRequest) (*coin_service.CoinsListResponse, error) {
	if err := request.Validate(); err != nil {
		return nil, twirp.InvalidArgumentError(err.Error(), "")
	}

	tx, err := s.DB.Begin()
	if err != nil {
		return nil, twirp.InternalError(err.Error())
	}

	coinsInPot, err := models.CoinsInPotsByPot_id(tx, int(request.PotId))
	if err != nil {
		return nil, twirp.NotFoundError(err.Error())
	}
	response := &coin_service.CoinsListResponse{
		Coins: make([]*coin_service.Coins, 0, len(coinsInPot)),
	}
	for _, coins := range coinsInPot {
		response.Coins = append(response.Coins, &coin_service.Coins{
			Kind:  coin_service.Coins_Kind(coins.Denomination),
			Count: coins.CoinCount,
		})
	}
	err = tx.Commit()
	if err != nil {
		return nil, twirp.InternalError(err.Error())
	}

	return response, nil
}
